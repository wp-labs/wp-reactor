use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime};
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use wf_lang::{BaseType, FieldType};
use wp_model_core::model::{
    DataRecord, DataType, DateTimeValue, Field, FieldStorage, HexT, Value as ModelValue,
    types::value::ObjectValue,
};

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::CloseReason;
use crate::match_engine::EngineHashMap;
use crate::match_engine::Value;
use crate::time::normalize_epoch_timestamp_float_nanos;

pub use wf_lang::wfu_meta::{
    WFU_CLOSE_REASON, WFU_EMIT_TIME, WFU_ENTITY_ID, WFU_ENTITY_TYPE, WFU_FIRED_AT, WFU_ID,
    WFU_ORIGIN, WFU_PREFIX, WFU_RULE_NAME, WFU_SCORE, WFU_SUMMARY,
};

/// Which path produced this alert.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq)]
#[moju(kind = "state", domain = "Engine", module = "Engine.AlertOutput")]
pub enum AlertOrigin {
    Event,
    Close { reason: CloseReason },
}

impl AlertOrigin {
    /// Canonical string form: `"event"`, `"close:timeout"`, `"close:flush"`, `"close:eos"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertOrigin::Event => "event",
            AlertOrigin::Close { reason } => match reason {
                CloseReason::Timeout => "close:timeout",
                CloseReason::Flush => "close:flush",
                CloseReason::Eos => "close:eos",
            },
        }
    }

    pub fn close_reason(&self) -> Option<CloseReason> {
        match self {
            AlertOrigin::Event => None,
            AlertOrigin::Close { reason } => Some(*reason),
        }
    }
}

impl fmt::Display for AlertOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for AlertOrigin {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AlertOrigin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "event" => Ok(AlertOrigin::Event),
            "close:timeout" => Ok(AlertOrigin::Close {
                reason: CloseReason::Timeout,
            }),
            "close:flush" => Ok(AlertOrigin::Close {
                reason: CloseReason::Flush,
            }),
            "close:eos" => Ok(AlertOrigin::Close {
                reason: CloseReason::Eos,
            }),
            other => Err(serde::de::Error::custom(format!(
                "unknown AlertOrigin: {other}"
            ))),
        }
    }
}

/// An output record produced by [`RuleExecutor`](crate::match_engine::RuleExecutor)
/// when the CEP state machine signals a match or close.
///
/// Plan-constant strings (`rule_name` / `entity_type` / `summary` on the
/// `on each` path / `yield_target` / `scope_key` on the `on each` path) and
/// the typed yield field list are shared via `Arc` — producing a record
/// clones references, not heap strings.
#[derive(::moju_derive::MoJu, Debug, Clone, serde::Serialize)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.AlertOutput")]
pub struct OutputRecord {
    /// SHA-256 content hash (16 hex).
    pub wfx_id: String,
    /// Name of the rule that fired.
    pub rule_name: Arc<str>,
    /// Score in `[0, 100]`, clamped.
    pub score: f64,
    /// Entity type from `EntityPlan` (e.g. `"ip"`).
    pub entity_type: Arc<str>,
    /// Entity id evaluated from `entity_id_expr`.
    pub entity_id: String,
    /// Which path produced this alert.
    pub origin: AlertOrigin,
    /// ISO 8601 UTC timestamp (`SystemTime`-based, no chrono).
    pub fired_at: String,
    /// ISO 8601 UTC timestamp when the engine emitted the record.
    /// `Arc<str>`: batch-level shared (rule_task caches the wall clock per
    /// batch), so all events in a batch share one allocation.
    pub emit_time: Arc<str>,
    /// Matched rows — always `vec![]` for L1 (placeholder for M25 join).
    #[serde(skip)]
    pub matched_rows: Vec<RecordBatch>,
    /// Human-readable summary of the alert.
    pub summary: Arc<str>,
    /// Yield target window name, used for sink routing.
    #[serde(skip)]
    pub yield_target: Arc<str>,
    /// Evaluated `yield (...)` fields, used by internal pipeline stages.
    #[serde(skip)]
    pub yield_fields: Vec<(Arc<str>, Value)>,
    /// Resolved types for `yield_fields`, aligned by field name when available.
    #[serde(skip)]
    pub yield_field_types: Arc<[(Arc<str>, FieldType)]>,
    /// Event-time for this output (nanos since epoch), used by internal windows.
    #[serde(skip)]
    pub event_time_nanos: i64,
    /// Machine identifier extracted from matched event (metrics-only, not yielded).
    #[serde(skip)]
    pub machine_id: String,
    /// State-machine scope key, formatted as `k1=v1,k2=v2` (metrics-only).
    #[serde(skip)]
    pub scope_key: Arc<str>,
}

impl OutputRecord {
    pub fn to_data_record(&self) -> CoreResult<DataRecord> {
        let mut record = DataRecord::default();

        // The system fields are distinct compile-time constants and yield
        // fields are rejected below when they use the reserved `__wfu_`
        // prefix, so the only duplicate check that can ever fire is
        // yield-vs-yield — handled with a short linear scan below. (This used
        // to be a `HashSet<String>` that allocated and SipHashed every field
        // of every emitted record, which profiled hot at ~3M records/s.)
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_ID,
            ModelValue::from(self.wfx_id.as_str()),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_RULE_NAME,
            ModelValue::from(&*self.rule_name),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Float,
            WFU_SCORE,
            ModelValue::from(self.score),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_ENTITY_TYPE,
            ModelValue::from(&*self.entity_type),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_ENTITY_ID,
            ModelValue::from(self.entity_id.as_str()),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_ORIGIN,
            ModelValue::from(self.origin.as_str()),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_CLOSE_REASON,
            ModelValue::from(
                self.origin
                    .close_reason()
                    .map_or("", |reason| reason.as_str()),
            ),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_FIRED_AT,
            ModelValue::from(self.fired_at.as_str()),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_EMIT_TIME,
            ModelValue::from(&*self.emit_time),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            WFU_SUMMARY,
            ModelValue::from(&*self.summary),
        )));

        for (idx, (name, value)) in self.yield_fields.iter().enumerate() {
            if name.starts_with(WFU_PREFIX) {
                return CoreReason::DataFormat
                    .to_err()
                    .with_detail(format!(
                        "yield field {name:?} uses reserved prefix {WFU_PREFIX}"
                    ))
                    .err();
            }
            if self.yield_fields[..idx].iter().any(|(prev, _)| prev == name) {
                return CoreReason::DataFormat
                    .to_err()
                    .with_detail(format!("duplicate exported field {name:?}"))
                    .err();
            }
            let field_type = self
                .yield_field_types
                .iter()
                .find_map(|(field_name, field_type)| (field_name == name).then_some(field_type));
            let (meta, model_value) = export_yield_value(value, field_type)?;
            record.push(FieldStorage::from_owned(Field::new(
                meta,
                name.as_ref(),
                model_value,
            )));
        }

        Ok(record)
    }
}

pub fn data_record_to_json_string(record: &DataRecord) -> CoreResult<String> {
    let mut obj = serde_json::Map::new();
    for field in &record.items {
        if field.get_meta() == &DataType::Ignore {
            continue;
        }
        obj.insert(
            field.get_name().to_string(),
            model_value_to_json(field.get_value()),
        );
    }
    serde_json::to_string(&serde_json::Value::Object(obj))
        .source_err(CoreReason::DataFormat, "serialize alert record to json")
}

pub(crate) fn export_yield_value(
    value: &Value,
    field_type: Option<&FieldType>,
) -> CoreResult<(DataType, ModelValue)> {
    match field_type {
        Some(FieldType::ArrayAny) => export_array_value(value, "auto"),
        Some(FieldType::Array(base_type)) => export_typed_array_value(value, base_type),
        Some(FieldType::Object) => export_object_value(value),
        Some(FieldType::Base(base_type)) => export_typed_value(base_type, value),
        None => export_untyped_value(value),
    }
}

fn export_typed_value(base_type: &BaseType, value: &Value) -> CoreResult<(DataType, ModelValue)> {
    match base_type {
        BaseType::Digit => match value {
            Value::Number(n) if n.is_finite() && n.fract() == 0.0 => {
                Ok((DataType::Digit, ModelValue::from(*n as i64)))
            }
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail("digit field requires an integer-compatible number")
                .err(),
        },
        BaseType::Float => match value {
            Value::Number(n) if n.is_finite() => Ok((DataType::Float, ModelValue::from(*n))),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail("float field requires a finite number")
                .err(),
        },
        BaseType::Bool => match value {
            Value::Bool(b) => Ok((DataType::Bool, ModelValue::from(*b))),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail("bool field requires a boolean")
                .err(),
        },
        BaseType::Chars => Ok((
            DataType::Chars,
            ModelValue::from(render_value_as_string(value)?.as_str()),
        )),
        BaseType::Time => {
            let dt = parse_time_value(value)?;
            Ok((DataType::Time, ModelValue::from(dt)))
        }
        BaseType::Ip => {
            let ip = parse_ip_value(value)?;
            Ok((DataType::IP, ModelValue::from(ip)))
        }
        BaseType::Hex => {
            let hex = parse_hex_value(value)?;
            Ok((DataType::Hex, ModelValue::from(hex)))
        }
    }
}

fn export_untyped_value(value: &Value) -> CoreResult<(DataType, ModelValue)> {
    match value {
        Value::Number(n) if n.is_finite() => Ok((DataType::Float, ModelValue::from(*n))),
        Value::Bool(b) => Ok((DataType::Bool, ModelValue::from(*b))),
        Value::Str(s) => Ok((DataType::Chars, ModelValue::from(s.as_str()))),
        Value::Array(_) => export_array_value(value, "auto"),
        Value::Object(_) => export_object_value(value),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("unsupported untyped yield value")
            .err(),
    }
}

fn render_value_as_string(value: &Value) -> CoreResult<String> {
    match value {
        Value::Str(s) => Ok(s.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Array(_) | Value::Object(_) => structured_json_string(value),
    }
}

fn export_array_value(value: &Value, item_type: &str) -> CoreResult<(DataType, ModelValue)> {
    match value {
        Value::Array(items) => Ok((
            DataType::Array(item_type.to_string()),
            ModelValue::Array(
                items
                    .iter()
                    .map(rule_value_to_array_item_storage)
                    .collect::<CoreResult<Vec<_>>>()?,
            ),
        )),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("array export expects an array value")
            .err(),
    }
}

fn export_typed_array_value(
    value: &Value,
    base_type: &BaseType,
) -> CoreResult<(DataType, ModelValue)> {
    match value {
        Value::Array(items) => Ok((
            DataType::Array(base_type_name(base_type).to_string()),
            ModelValue::Array(
                items
                    .iter()
                    .map(|item| rule_value_to_typed_field_storage("item", base_type, item))
                    .collect::<CoreResult<Vec<_>>>()?,
            ),
        )),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("array export expects an array value")
            .err(),
    }
}

fn export_object_value(value: &Value) -> CoreResult<(DataType, ModelValue)> {
    match value {
        Value::Object(items) => Ok((DataType::Obj, ModelValue::Obj(rule_object_to_model(items)?))),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("object export expects an object value")
            .err(),
    }
}

fn structured_json_string(value: &Value) -> CoreResult<String> {
    match value {
        Value::Array(_) | Value::Object(_) => serde_json::to_string(&rule_value_to_json(value)?)
            .source_err(CoreReason::DataFormat, "serialize structured yield value"),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("structured string rendering expects an array or object value")
            .err(),
    }
}

fn rule_value_to_named_field_storage(name: &str, value: &Value) -> CoreResult<FieldStorage> {
    let (meta, model_value) = rule_value_to_model_value(value)?;
    Ok(FieldStorage::from_owned(Field::new(
        meta,
        name,
        model_value,
    )))
}

fn rule_value_to_array_item_storage(value: &Value) -> CoreResult<FieldStorage> {
    rule_value_to_named_field_storage("item", value)
}

fn rule_value_to_typed_field_storage(
    name: &str,
    base_type: &BaseType,
    value: &Value,
) -> CoreResult<FieldStorage> {
    let (meta, model_value) = export_typed_array_item_value(base_type, value)?;
    Ok(FieldStorage::from_owned(Field::new(
        meta,
        name,
        model_value,
    )))
}

fn export_typed_array_item_value(
    base_type: &BaseType,
    value: &Value,
) -> CoreResult<(DataType, ModelValue)> {
    match base_type {
        BaseType::Chars => match value {
            Value::Str(s) => Ok((DataType::Chars, ModelValue::from(s.as_str()))),
            _ => CoreReason::DataFormat
                .to_err()
                .with_detail("array/chars field requires string elements")
                .err(),
        },
        _ => export_typed_value(base_type, value),
    }
}

fn rule_value_to_model_value(value: &Value) -> CoreResult<(DataType, ModelValue)> {
    match value {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 => {
            Ok((DataType::Digit, ModelValue::from(*n as i64)))
        }
        Value::Number(n) if n.is_finite() => Ok((DataType::Float, ModelValue::from(*n))),
        Value::Str(s) => Ok((DataType::Chars, ModelValue::from(s.as_str()))),
        Value::Bool(b) => Ok((DataType::Bool, ModelValue::from(*b))),
        Value::Array(items) => Ok((
            DataType::Array("auto".to_string()),
            ModelValue::Array(
                items
                    .iter()
                    .map(rule_value_to_array_item_storage)
                    .collect::<CoreResult<Vec<_>>>()?,
            ),
        )),
        Value::Object(items) => Ok((DataType::Obj, ModelValue::Obj(rule_object_to_model(items)?))),
        Value::Number(_) => CoreReason::DataFormat
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
    }
}

fn rule_object_to_model(
    items: &EngineHashMap<smol_str::SmolStr, Value>,
) -> CoreResult<ObjectValue> {
    let mut object = ObjectValue::new();
    for (key, value) in items {
        object.insert(key.as_str(), rule_value_to_named_field_storage(key, value)?);
    }
    Ok(object)
}

fn rule_value_to_json(value: &Value) -> CoreResult<serde_json::Value> {
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
                .map(rule_value_to_json)
                .collect::<CoreResult<Vec<_>>>()?,
        )),
        Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), rule_value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

fn model_value_to_json(value: &ModelValue) -> serde_json::Value {
    match value {
        ModelValue::Null => serde_json::Value::Null,
        ModelValue::Bool(v) => serde_json::Value::from(*v),
        ModelValue::Chars(v) => serde_json::Value::from(v.to_string()),
        ModelValue::Float(v) => serde_json::Value::from(*v),
        ModelValue::Digit(v) => serde_json::Value::from(*v),
        ModelValue::Obj(v) => serde_json::Value::Object(
            v.iter()
                .map(|(key, field)| (key.to_string(), model_value_to_json(field.get_value())))
                .collect(),
        ),
        ModelValue::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|field| model_value_to_json(field.get_value()))
                .collect(),
        ),
        other => serde_json::Value::from(other.to_string()),
    }
}

fn base_type_name(base_type: &BaseType) -> &'static str {
    match base_type {
        BaseType::Digit => "digit",
        BaseType::Float => "float",
        BaseType::Bool => "bool",
        BaseType::Chars => "chars",
        BaseType::Time => "time",
        BaseType::Ip => "ip",
        BaseType::Hex => "hex",
    }
}

fn parse_time_value(value: &Value) -> CoreResult<DateTimeValue> {
    match value {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 => {
            let nanos = normalize_epoch_timestamp_float_nanos(*n).ok_or_else(|| {
                orion_error::StructError::from(CoreReason::DataFormat)
                    .with_detail("time field requires a finite epoch timestamp")
            })?;
            Ok(DateTime::from_timestamp_nanos(nanos).naive_utc())
        }
        Value::Str(text) => parse_time_text(text),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("time field requires RFC3339 text or an epoch timestamp number")
            .err(),
    }
}

fn parse_time_text(text: &str) -> CoreResult<DateTimeValue> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Ok(dt.naive_utc());
    }

    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(text, fmt) {
            return Ok(dt);
        }
    }

    CoreReason::DataFormat
        .to_err()
        .with_detail(format!("invalid time literal {text:?}"))
        .err()
}

fn parse_ip_value(value: &Value) -> CoreResult<IpAddr> {
    match value {
        Value::Str(text) => IpAddr::from_str(text).source_raw_err(
            CoreReason::DataFormat,
            format!("invalid ip literal {text:?}"),
        ),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("ip field requires string input")
            .err(),
    }
}

fn parse_hex_value(value: &Value) -> CoreResult<HexT> {
    match value {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 && *n >= 0.0 => Ok(HexT(*n as u128)),
        Value::Str(text) => {
            let normalized = text
                .strip_prefix("0x")
                .or_else(|| text.strip_prefix("0X"))
                .unwrap_or(text);
            let parsed = u128::from_str_radix(normalized, 16).source_raw_err(
                CoreReason::DataFormat,
                format!("invalid hex literal {text:?}"),
            )?;
            Ok(HexT(parsed))
        }
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("hex field requires hex string or non-negative integer")
            .err(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_data_record_exports_prefixed_system_fields_and_yield_values() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 70.5,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Close {
                reason: CloseReason::Timeout,
            },
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![
                ("count".into(), Value::Number(3.0)),
                (
                    "items".into(),
                    Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]),
                ),
                (
                    "risk_context".into(),
                    Value::Object(
                        [
                            ("score".into(), Value::Number(70.5)),
                            (
                                "tags".into(),
                                Value::Array(vec![
                                    Value::Str("bruteforce".into()),
                                    Value::Str("ssh".into()),
                                ]),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                ),
            ],
            yield_field_types: std::sync::Arc::from(vec![
                ("count".into(), FieldType::Base(BaseType::Digit)),
                ("items".into(), FieldType::Array(BaseType::Chars)),
                ("risk_context".into(), FieldType::Object),
            ]),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let record = output.to_data_record().expect("record");
        assert_eq!(
            record.get_value(WFU_RULE_NAME),
            Some(&ModelValue::from("rule-a"))
        );
        assert_eq!(
            record.get_value(WFU_CLOSE_REASON),
            Some(&ModelValue::from("timeout"))
        );
        assert_eq!(record.get_value("count"), Some(&ModelValue::from(3_i64)));
        assert!(matches!(
            record.get_value("items"),
            Some(ModelValue::Array(_))
        ));
        assert!(matches!(
            record.get_value("risk_context"),
            Some(ModelValue::Obj(_))
        ));

        let json = data_record_to_json_string(&record).expect("json");
        let json: serde_json::Value = serde_json::from_str(&json).expect("json value");
        assert_eq!(json["__wfu_rule_name"], "rule-a");
        assert_eq!(json["items"], serde_json::json!(["a", "b"]));
        assert_eq!(json["risk_context"]["score"], 70.5);
        assert_eq!(
            json["risk_context"]["tags"],
            serde_json::json!(["bruteforce", "ssh"])
        );
    }

    #[test]
    fn to_data_record_rejects_reserved_prefix_in_yield_fields() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Event,
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![(std::sync::Arc::from(format!("{WFU_PREFIX}bad").as_str()), Value::Str("x".into()))],
            yield_field_types: Vec::new().into(),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let err = output
            .to_data_record()
            .expect_err("reserved prefix should fail");
        assert!(err.to_string().contains(WFU_PREFIX));
    }

    #[test]
    fn to_data_record_rejects_duplicate_yield_fields() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Event,
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![
                ("count".into(), Value::Number(1.0)),
                ("other".into(), Value::Number(2.0)),
                ("count".into(), Value::Number(3.0)),
            ],
            yield_field_types: Vec::new().into(),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let err = output
            .to_data_record()
            .expect_err("duplicate yield field should fail");
        assert!(err.to_string().contains("duplicate exported field"));
    }

    #[test]
    fn parse_time_value_accepts_epoch_milliseconds() {
        let dt = parse_time_value(&Value::Number(1_710_115_200_123.0)).expect("time");
        assert_eq!(
            dt,
            DateTime::from_timestamp_millis(1_710_115_200_123)
                .expect("millis")
                .naive_utc()
        );
    }

    #[test]
    fn data_record_json_keeps_non_json_chars_as_strings() {
        let mut record = DataRecord::default();
        let mut push = |record: &mut DataRecord, name: &str, value: &str| {
            record.push(FieldStorage::from_owned(Field::new(
                DataType::Chars,
                name,
                ModelValue::from(value),
            )));
        };
        push(&mut record, "text", "{not-json}");
        push(&mut record, "json_array_text", "[]");
        push(&mut record, "json_object_text", r#"{"raw":true}"#);
        push(&mut record, "plain_array_text", "[not-json]");

        let json = data_record_to_json_string(&record).expect("json");
        let json: serde_json::Value = serde_json::from_str(&json).expect("json value");

        assert_eq!(json["text"], "{not-json}");
        assert_eq!(json["json_array_text"], "[]");
        assert_eq!(json["json_object_text"], r#"{"raw":true}"#);
        assert_eq!(json["plain_array_text"], "[not-json]");
    }

    #[test]
    fn data_record_to_json_string_skips_ignore_fields() {
        let mut record = DataRecord::default();
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Ignore,
            "__wfu_rule_name",
            ModelValue::from("rule-a"),
        )));
        record.push(FieldStorage::from_owned(Field::new(
            DataType::Chars,
            "message",
            ModelValue::from("hello"),
        )));

        let json = data_record_to_json_string(&record).expect("json");
        let json: serde_json::Value = serde_json::from_str(&json).expect("json value");

        assert!(json.get("__wfu_rule_name").is_none());
        assert_eq!(json["message"], "hello");
    }

    #[test]
    fn structured_object_string_rendering_sorts_keys() {
        let value = Value::Object(
            [
                ("z".into(), Value::Str("last".into())),
                ("a".into(), Value::Str("first".into())),
            ]
            .into_iter()
            .collect(),
        );

        let rendered = structured_json_string(&value).expect("json string");
        assert_eq!(rendered, r#"{"a":"first","z":"last"}"#);
    }

    #[test]
    fn to_data_record_rejects_typed_array_element_mismatch() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Event,
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![(
                "scores".into(),
                Value::Array(vec![Value::Number(1.0), Value::Str("high".into())]),
            )],
            yield_field_types: std::sync::Arc::from(vec![("scores".into(), FieldType::Array(BaseType::Float))]),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let err = output
            .to_data_record()
            .expect_err("typed array element mismatch should fail");
        assert!(err.to_string().contains("float field requires"));
    }

    #[test]
    fn to_data_record_rejects_array_chars_non_string_element() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Event,
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![(
                "tags".into(),
                Value::Array(vec![Value::Str("ssh".into()), Value::Number(22.0)]),
            )],
            yield_field_types: std::sync::Arc::from(vec![("tags".into(), FieldType::Array(BaseType::Chars))]),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let err = output
            .to_data_record()
            .expect_err("array/chars should reject non-string elements");
        assert!(err.to_string().contains("array/chars"));
    }

    #[test]
    fn to_data_record_rejects_non_finite_number_inside_structured_value() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Event,
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![(
                "risk_context".into(),
                Value::Object(
                    [("score".into(), Value::Number(f64::NAN))]
                        .into_iter()
                        .collect(),
                ),
            )],
            yield_field_types: std::sync::Arc::from(vec![("risk_context".into(), FieldType::Object)]),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let err = output
            .to_data_record()
            .expect_err("non-finite structured number should fail");
        assert!(
            err.to_string()
                .contains("structured numeric value must be finite")
        );
    }

    #[test]
    fn to_data_record_preserves_ip_time_and_hex_types() {
        let output = OutputRecord {
            wfx_id: "id-1".into(),
            rule_name: "rule-a".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "1.1.1.1".into(),
            origin: AlertOrigin::Event,
            fired_at: "2026-03-11T00:00:00.000Z".into(),
            emit_time: "2026-03-11T00:00:01.000Z".into(),
            matched_rows: vec![],
            summary: "demo".into(),
            yield_target: "out".into(),
            yield_fields: vec![
                ("src_ip".into(), Value::Str("192.168.0.1".into())),
                (
                    "seen_at".into(),
                    Value::Number(1_710_115_200_000_000_000_f64),
                ),
                ("sha".into(), Value::Str("0xFF".into())),
            ],
            yield_field_types: std::sync::Arc::from(vec![
                ("src_ip".into(), FieldType::Base(BaseType::Ip)),
                ("seen_at".into(), FieldType::Base(BaseType::Time)),
                ("sha".into(), FieldType::Base(BaseType::Hex)),
            ]),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        };

        let record = output.to_data_record().expect("record");
        assert_eq!(
            record.field("src_ip").expect("ip").get_meta(),
            &DataType::IP
        );
        assert_eq!(
            record.field("seen_at").expect("time").get_meta(),
            &DataType::Time
        );
        assert_eq!(record.field("sha").expect("hex").get_meta(), &DataType::Hex);
    }
}
