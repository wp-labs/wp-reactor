mod alert;
mod close_exec;
mod context;
mod each_exec;
mod eval;
mod match_exec;

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

use orion_error::conversion::{SourceRawErr, ToStructError};
use wf_config::OutputConfig;
use wf_lang::ast::Expr;
use wf_lang::plan::RulePlan;
use wf_lang::{BaseType, FieldType};

use self::eval::eval_bool_expr_with_lookup;
use crate::error::{CoreReason, CoreResult};
use crate::match_engine::match_engine::{Event, Value, WindowLookup};
use crate::time::normalize_epoch_timestamp_float_nanos;

/// Evaluates score/entity expressions from a [`RulePlan`] and produces
/// [`OutputRecord`]s from CEP match/close outputs.
///
/// L1 rules use `execute_match` / `execute_close` (no joins).
/// L2 rules with joins use `execute_match_with_joins` / `execute_close_with_joins`
/// which accept a [`WindowLookup`] for resolving join data.
#[derive(::moju_derive::MoJu, Clone)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct RuleExecutor {
    plan: RulePlan,
    yield_field_types: HashMap<String, FieldType>,
    output: OutputConfig,
    /// alias → bind filter, precomputed so per-event alias matching is O(1)
    /// instead of a linear scan of `plan.binds` on every (event × alias).
    bind_filters: HashMap<String, Option<Expr>>,
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
        Self {
            plan,
            yield_field_types: options.yield_field_types,
            output: options.output,
            bind_filters,
        }
    }

    pub fn plan(&self) -> &RulePlan {
        &self.plan
    }

    pub(crate) fn yield_field_type(&self, name: &str) -> Option<&FieldType> {
        self.yield_field_types.get(name)
    }

    pub(crate) fn output_config(&self) -> &OutputConfig {
        &self.output
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
