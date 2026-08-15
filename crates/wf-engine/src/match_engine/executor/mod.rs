mod alert;
mod close_exec;
mod context;
mod each_exec;
mod eval;
mod match_exec;

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use orion_error::conversion::{SourceRawErr, ToStructError};
use wf_config::OutputConfig;
use wf_lang::ast::Expr;
use wf_lang::plan::RulePlan;
use wf_lang::{BaseType, FieldType};

use self::alert::build_summary;
use self::eval::eval_bool_expr_with_lookup;
use crate::alert::AlertOrigin;
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::match_engine::{Event, Value, WindowLookup};
use crate::time::normalize_epoch_timestamp_float_nanos;

/// Plan-level output constants, precomputed once at executor construction.
///
/// These are identical for every event/match a rule produces. The hot path
/// previously re-derived them per event — `String` clones of rule/entity/
/// target names, per-field `HashMap` type lookups, per-event summary
/// formatting — roughly a dozen heap allocations per output record that
/// existed only to reproduce plan constants.
#[derive(Clone)]
pub(crate) struct OutputStatic {
    pub(crate) rule_name: Arc<str>,
    pub(crate) entity_type: Arc<str>,
    pub(crate) yield_target: Arc<str>,
    /// `(field name, resolved type)` aligned by index with
    /// `plan.yield_plan.fields` — kills the per-field type lookup + name
    /// clone on every output.
    pub(crate) yield_specs: Arc<[(Arc<str>, Option<FieldType>)]>,
    /// Typed field list carried by every `OutputRecord` (plan constant).
    pub(crate) yield_field_types: Arc<[(Arc<str>, FieldType)]>,
    /// `on each` constant summary — scope key and step data are always empty
    /// on that path, so the whole summary string is a plan constant.
    pub(crate) each_summary: Option<Arc<str>>,
}

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`OutputRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct RuleExecutor {
    plan: RulePlan,
    yield_field_types: HashMap<String, FieldType>,
    output: OutputConfig,
    /// alias → bind filter, precomputed so per-event alias matching is O(1)
    /// instead of a linear scan of `plan.binds` on every (event × alias).
    bind_filters: HashMap<String, Option<Expr>>,
    output_static: OutputStatic,
    /// Last (nanos, formatted) emit time. The runtime feeds a batch-level
    /// cached wall clock into the on-each path, so all events in a batch
    /// share one timestamp — format it once and Arc-share it instead of one
    /// String per event.
    ///
    /// The cache is a pure memo (value fully determined by the nanos key),
    /// so clones get their OWN cache (reset to empty). It must NOT be
    /// shared behind an `Arc`: sharded on-each workers lock it per event,
    /// and a shared `Mutex` ping-pongs a cache line across worker threads —
    /// 6 workers on one lock dropped per-worker throughput ~20x (nexmark
    /// q1 30M, 2026-08-16).
    emit_time_cache: Mutex<(i64, Arc<str>)>,
}

// Manual impl: `Mutex` is not `Clone`. `emit_time_cache` is a pure memo
// keyed by nanos, so the clone simply starts with an empty cache instead of
// sharing the lock — each sharded on-each worker locks only its own cache.
impl Clone for RuleExecutor {
    fn clone(&self) -> Self {
        Self {
            plan: self.plan.clone(),
            yield_field_types: self.yield_field_types.clone(),
            output: self.output.clone(),
            bind_filters: self.bind_filters.clone(),
            output_static: self.output_static.clone(),
            emit_time_cache: Mutex::new((0, Arc::from(""))),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuleExecutorOptions {
    pub yield_field_types: HashMap<String, FieldType>,
    pub output: OutputConfig,
}

impl RuleExecutor {
    pub fn new(plan: RulePlan) -> Self {
        Self::new_with_options(plan, RuleExecutorOptions::default())
    }

    pub fn new_with_yield_field_types(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
    ) -> Self {
        Self::new_with_options(
            plan,
            RuleExecutorOptions {
                yield_field_types,
                output: OutputConfig::default(),
            },
        )
    }

    pub fn new_with_yield_field_types_and_output(
        plan: RulePlan,
        yield_field_types: HashMap<String, FieldType>,
        output: OutputConfig,
    ) -> Self {
        Self::new_with_options(
            plan,
            RuleExecutorOptions {
                yield_field_types,
                output,
            },
        )
    }

    pub fn new_with_options(plan: RulePlan, options: RuleExecutorOptions) -> Self {
        let bind_filters = plan
            .binds
            .iter()
            .map(|b| (b.alias.clone(), b.filter.clone()))
            .collect();
        // Precompute plan-level output constants (see `OutputStatic`). The
        // yield field types map comes from runtime schema knowledge, which is
        // exactly what `new_with_options` receives.
        let yield_specs: Vec<(Arc<str>, Option<FieldType>)> = plan
            .yield_plan
            .fields
            .iter()
            .map(|field| {
                (
                    Arc::from(field.name.as_str()),
                    options.yield_field_types.get(&field.name).cloned(),
                )
            })
            .collect();
        let typed_fields: Vec<(Arc<str>, FieldType)> = yield_specs
            .iter()
            .filter_map(|(name, field_type)| {
                field_type
                    .clone()
                    .map(|field_type| (Arc::clone(name), field_type))
            })
            .collect();
        let each_summary = plan.each_plan.as_ref().map(|_| {
            Arc::from(build_summary(
                &plan.name,
                &[],
                &[],
                &[],
                &AlertOrigin::Event,
            ))
        });
        Self {
            output_static: OutputStatic {
                rule_name: Arc::from(plan.name.as_str()),
                entity_type: Arc::from(plan.entity_plan.entity_type.as_str()),
                yield_target: Arc::from(plan.yield_plan.target.as_str()),
                yield_specs: Arc::from(yield_specs),
                yield_field_types: Arc::from(typed_fields),
                each_summary,
            },
            plan,
            yield_field_types: options.yield_field_types,
            output: options.output,
            bind_filters,
            emit_time_cache: Mutex::new((0, Arc::from(""))),
        }
    }

    /// Formatted emit time for `nanos`, cached: consecutive calls with the
    /// same nanos (the batch-shared wall clock) return the same `Arc<str>`
    /// with no re-formatting.
    pub(crate) fn cached_emit_time(&self, nanos: i64) -> Arc<str> {
        let mut cache = self.emit_time_cache.lock().unwrap();
        if cache.0 != nanos || cache.1.is_empty() {
            *cache = (nanos, Arc::from(alert::format_nanos_utc(nanos)));
        }
        Arc::clone(&cache.1)
    }

    pub fn plan(&self) -> &RulePlan {
        &self.plan
    }

    pub(crate) fn yield_field_type(&self, name: &str) -> Option<&FieldType> {
        self.yield_field_types.get(name)
    }

    /// Precomputed plan-level output constants (see [`OutputStatic`]).
    pub(crate) fn output_static(&self) -> &OutputStatic {
        &self.output_static
    }

    pub(crate) fn output_config(&self) -> &OutputConfig {
        &self.output
    }

    /// Coerce a yield field value against a precomputed type (from
    /// `output_static().yield_specs`) — avoids the per-field `HashMap`
    /// lookup on the hot path.
    pub(crate) fn coerce_yield_field_value_with(
        name: &str,
        field_type: Option<&FieldType>,
        value: Value,
    ) -> CoreResult<Option<Value>> {
        let Some(field_type) = field_type else {
            return Ok(Some(value));
        };
        coerce_yield_value(name, field_type, value)
    }

    /// Coerce a yield field value against its target type. Returns `Ok(None)`
    /// when the field should be omitted from the output (an optional input
    /// field that was missing at evaluation time), `Ok(Some(v))` on success,
    /// and `Err` on genuine type/format errors.
    pub(crate) fn coerce_yield_field_value(
        &self,
        name: &str,
        value: Value,
    ) -> CoreResult<Option<Value>> {
        let Some(field_type) = self.yield_field_type(name) else {
            return Ok(Some(value));
        };
        coerce_yield_value(name, field_type, value)
    }

    pub(crate) fn build_machine_id(&self, machine_id: &str) -> String {
        if machine_id.is_empty() {
            self.plan.name.clone()
        } else {
            machine_id.to_string()
        }
    }

    pub(crate) fn build_scope_key(
        &self,
        keys: &[wf_lang::ast::FieldRef],
        scope_values: &[crate::match_engine::match_engine::Value],
    ) -> String {
        keys.iter()
            .zip(scope_values.iter())
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    crate::match_engine::match_engine::field_ref_name(k),
                    crate::match_engine::match_engine::value_to_string(v)
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn event_matches_alias(
        &self,
        alias: &str,
        event: &Event,
        windows: Option<&dyn WindowLookup>,
    ) -> bool {
        // Few binds: a linear scan is cheaper than hashing the alias. Many binds:
        // the precomputed map keeps this O(1) instead of O(binds) per event.
        // Measured crossover: the map wins from ~24 binds (24: 5.1M vs 5.8M q/s;
        // 16: linear still 1.3x faster).
        let filter = if self.plan.binds.len() <= 24 {
            self.plan
                .binds
                .iter()
                .find(|b| b.alias == alias)
                .and_then(|b| b.filter.as_ref())
        } else {
            self.bind_filters.get(alias).and_then(|f| f.as_ref())
        };
        passes_bind_filter(filter, event, windows)
    }

    pub fn is_aux_bind_alias(&self, alias: &str) -> bool {
        !self
            .plan
            .match_plan
            .event_steps
            .iter()
            .chain(self.plan.match_plan.close_steps.iter())
            .flat_map(|step| step.branches.iter())
            .any(|branch| branch.source == alias)
    }
}

fn coerce_yield_value(
    name: &str,
    field_type: &FieldType,
    value: Value,
) -> CoreResult<Option<Value>> {
    // A yield expression referencing a missing input field evaluates to the
    // empty-string fallback (see `eval_yield_expr_with_meta`). For targets that
    // can never be a valid empty string, treat it as an absent/optional field:
    // omit it from the output instead of failing the whole record
    // (wp-labs/warp-fusion#62). Explicit NaN/Infinity values still fail below.
    if matches!(&value, Value::Str(s) if s.is_empty())
        && !matches!(field_type, FieldType::Base(BaseType::Chars))
    {
        return Ok(None);
    }
    match field_type {
        FieldType::Base(base_type) => coerce_yield_base_value(name, base_type, value).map(Some),
        FieldType::Array(_) | FieldType::ArrayAny => match value {
            Value::Array(_) => Ok(Some(value)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an array value"))
                .err(),
        },
        FieldType::Object => match value {
            Value::Object(_) => Ok(Some(value)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an object value"))
                .err(),
        },
    }
}

fn coerce_yield_base_value(name: &str, base_type: &BaseType, value: Value) -> CoreResult<Value> {
    match base_type {
        BaseType::Chars => render_yield_value_as_string(value).map(|s| Value::Str(s.into())),
        BaseType::Digit => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 => Ok(Value::Number(n)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} expects an integer-compatible number"
                ))
                .err(),
        },
        BaseType::Float => match value {
            Value::Number(n) if n.is_finite() => Ok(Value::Number(n)),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects a finite number"))
                .err(),
        },
        BaseType::Bool => match value {
            Value::Bool(_) => Ok(value),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects a boolean value"))
                .err(),
        },
        BaseType::Time => coerce_yield_time_value(name, value),
        BaseType::Ip => match value {
            Value::Str(text) => {
                IpAddr::from_str(&text).source_raw_err(
                    CoreReason::DataFormat,
                    format!("yield field {name:?} has invalid ip literal {text:?}"),
                )?;
                Ok(Value::Str(text))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!("yield field {name:?} expects an ip string"))
                .err(),
        },
        BaseType::Hex => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 && n >= 0.0 => {
                Ok(Value::Number(n))
            }
            Value::Str(text) => {
                let normalized = text
                    .strip_prefix("0x")
                    .or_else(|| text.strip_prefix("0X"))
                    .unwrap_or(&text);
                u128::from_str_radix(normalized, 16).source_raw_err(
                    CoreReason::DataFormat,
                    format!("yield field {name:?} has invalid hex literal {text:?}"),
                )?;
                Ok(Value::Str(text))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "yield field {name:?} expects a hex string or non-negative integer"
                ))
                .err(),
        },
    }
}

fn coerce_yield_time_value(name: &str, value: Value) -> CoreResult<Value> {
    match value {
        Value::Number(n) => {
            normalize_epoch_timestamp_float_nanos(n).ok_or_else(|| {
                orion_error::StructError::from(CoreReason::DataFormat).with_detail(format!(
                    "yield field {name:?} expects a valid epoch timestamp"
                ))
            })?;
            Ok(Value::Number(n))
        }
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail(format!(
                "yield field {name:?} expects an explicit time expression or epoch timestamp"
            ))
            .err(),
    }
}

fn render_yield_value_as_string(value: Value) -> CoreResult<String> {
    match value {
        Value::Str(s) => Ok(s.to_string()),
        Value::Number(n) if n.is_finite() => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(&yield_value_to_json(&value)?)
            .source_raw_err(CoreReason::DataFormat, "serialize structured yield value"),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("yield string conversion requires finite numeric values")
            .err(),
    }
}

fn yield_value_to_json(value: &Value) -> CoreResult<serde_json::Value> {
    match value {
        Value::Number(n) if n.is_finite() => Ok(serde_json::Value::from(*n)),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
        Value::Str(s) => Ok(serde_json::Value::from(s.as_str())),
        Value::Bool(b) => Ok(serde_json::Value::from(*b)),
        Value::Array(items) => Ok(serde_json::Value::Array(
            items
                .iter()
                .map(yield_value_to_json)
                .collect::<CoreResult<Vec<_>>>()?,
        )),
        Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), yield_value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

fn passes_bind_filter(
    filter: Option<&Expr>,
    event: &Event,
    windows: Option<&dyn WindowLookup>,
) -> bool {
    match filter.and_then(|expr| eval_bool_expr_with_lookup(expr, event, windows)) {
        Some(result) => result,
        None => filter.is_none(),
    }
}
