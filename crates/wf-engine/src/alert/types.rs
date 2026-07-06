use std::collections::HashSet;
use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime};
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use wf_lang::{BaseType, FieldType};
use wp_model_core::model::{
    DataRecord, DataType, DateTimeValue, Field, FieldStorage, HexT, Value as ModelValue,
};

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::CloseReason;
use crate::match_engine::Value;

pub const WFU_ID: &str = "__wfu_id";
pub const WFU_RULE_NAME: &str = "__wfu_rule_name";
pub const WFU_SCORE: &str = "__wfu_score";
pub const WFU_ENTITY_TYPE: &str = "__wfu_entity_type";
pub const WFU_ENTITY_ID: &str = "__wfu_entity_id";
pub const WFU_ORIGIN: &str = "__wfu_origin";
pub const WFU_CLOSE_REASON: &str = "__wfu_close_reason";
pub const WFU_FIRED_AT: &str = "__wfu_fired_at";
pub const WFU_EMIT_TIME: &str = "__wfu_emit_time";
pub const WFU_SUMMARY: &str = "__wfu_summary";
pub const WFU_PREFIX: &str = "__wfu_";

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
#[derive(::moju_derive::MoJu, Debug, Clone, serde::Serialize)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.AlertOutput")]
pub struct OutputRecord {
    /// SHA-256 content hash (16 hex).
    pub wfx_id: String,
    /// Name of the rule that fired.
    pub rule_name: String,
    /// Score in `[0, 100]`, clamped.
    pub score: f64,
    /// Entity type from `EntityPlan` (e.g. `"ip"`).
    pub entity_type: String,
    /// Entity id evaluated from `entity_id_expr`.
    pub entity_id: String,
    /// Which path produced this alert.
    pub origin: AlertOrigin,
    /// ISO 8601 UTC timestamp (`SystemTime`-based, no chrono).
    pub fired_at: String,
    /// ISO 8601 UTC timestamp when the engine emitted the record.
    pub emit_time: String,
    /// Matched rows — always `vec![]` for L1 (placeholder for M25 join).
    #[serde(skip)]
    pub matched_rows: Vec<RecordBatch>,
    /// Human-readable summary of the alert.
    pub summary: String,
    /// Yield target window name, used for sink routing.
    #[serde(skip)]
    pub yield_target: String,
    /// Evaluated `yield (...)` fields, used by internal pipeline stages.
    #[serde(skip)]
    pub yield_fields: Vec<(String, Value)>,
    /// Resolved types for `yield_fields`, aligned by field name when available.
    #[serde(skip)]
    pub yield_field_types: Vec<(String, FieldType)>,
    /// Event-time for this output (nanos since epoch), used by internal windows.
    #[serde(skip)]
    pub event_time_nanos: i64,
    /// Machine identifier extracted from matched event (metrics-only, not yielded).
    #[serde(skip)]
    pub machine_id: String,
    /// State-machine scope key, formatted as `k1=v1,k2=v2` (metrics-only).
    #[serde(skip)]
    pub scope_key: String,
}

impl OutputRecord {
    pub fn to_data_record(&self) -> CoreResult<DataRecord> {
        let mut record = DataRecord::default();
        let mut exported = HashSet::new();

        append_field(
            &mut record,
            &mut exported,
            WFU_ID,
            DataType::Chars,
            ModelValue::from(self.wfx_id.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_RULE_NAME,
            DataType::Chars,
            ModelValue::from(self.rule_name.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_SCORE,
            DataType::Float,
            ModelValue::from(self.score),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_ENTITY_TYPE,
            DataType::Chars,
            ModelValue::from(self.entity_type.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_ENTITY_ID,
            DataType::Chars,
            ModelValue::from(self.entity_id.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_ORIGIN,
            DataType::Chars,
            ModelValue::from(self.origin.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_CLOSE_REASON,
            DataType::Chars,
            ModelValue::from(
                self.origin
                    .close_reason()
                    .map_or("", |reason| reason.as_str()),
            ),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_FIRED_AT,
            DataType::Chars,
            ModelValue::from(self.fired_at.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_EMIT_TIME,
            DataType::Chars,
            ModelValue::from(self.emit_time.as_str()),
        )?;
        append_field(
            &mut record,
            &mut exported,
            WFU_SUMMARY,
            DataType::Chars,
            ModelValue::from(self.summary.as_str()),
        )?;

        for (name, value) in &self.yield_fields {
            if name.starts_with(WFU_PREFIX) {
                return CoreReason::DataFormat
                    .to_err()
                    .with_detail(format!(
                        "yield field {name:?} uses reserved prefix {WFU_PREFIX}"
                    ))
                    .err();
            }
            let field_type = self
                .yield_field_types
                .iter()
                .find_map(|(field_name, field_type)| (field_name == name).then_some(field_type));
            let (meta, model_value) = export_yield_value(value, field_type)?;
            append_field(&mut record, &mut exported, name, meta, model_value)?;
        }

        Ok(record)
    }
}

pub fn data_record_to_json_string(record: &DataRecord) -> CoreResult<String> {
    let mut obj = serde_json::Map::new();
    for field in &record.items {
        obj.insert(
            field.get_name().to_string(),
            model_value_to_json(field.get_value()),
        );
    }
    serde_json::to_string(&serde_json::Value::Object(obj))
        .source_err(CoreReason::DataFormat, "serialize alert record to json")
}

fn append_field(
    record: &mut DataRecord,
    exported: &mut HashSet<String>,
    name: &str,
    meta: DataType,
    value: ModelValue,
) -> CoreResult<()> {
    if !exported.insert(name.to_string()) {
        return CoreReason::DataFormat
            .to_err()
            .with_detail(format!("duplicate exported field {name:?}"))
            .err();
    }
    record.push(FieldStorage::from_owned(Field::new(meta, name, value)));
    Ok(())
}

fn export_yield_value(
    value: &Value,
    field_type: Option<&FieldType>,
) -> CoreResult<(DataType, ModelValue)> {
    match field_type {
        Some(FieldType::Array(_)) => Ok((
            DataType::Chars,
            ModelValue::from(array_json_string(value)?.as_str()),
        )),
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
        Value::Array(_) => Ok((
            DataType::Chars,
            ModelValue::from(array_json_string(value)?.as_str()),
        )),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("unsupported untyped yield value")
            .err(),
    }
}

fn render_value_as_string(value: &Value) -> CoreResult<String> {
    match value {
        Value::Str(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Array(_) => array_json_string(value),
    }
}

fn array_json_string(value: &Value) -> CoreResult<String> {
    match value {
        Value::Array(_) => serde_json::to_string(&rule_value_to_json(value))
            .source_err(CoreReason::DataFormat, "serialize array yield value"),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("array export expects an array value")
            .err(),
    }
}

fn rule_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Number(n) => serde_json::Value::from(*n),
        Value::Str(s) => serde_json::Value::from(s.clone()),
        Value::Bool(b) => serde_json::Value::from(*b),
        Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(rule_value_to_json).collect())
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
        other => serde_json::Value::from(other.to_string()),
    }
}

fn parse_time_value(value: &Value) -> CoreResult<DateTimeValue> {
    match value {
        Value::Number(n) if n.is_finite() && n.fract() == 0.0 => {
            let nanos = *n as i64;
            Ok(DateTime::from_timestamp_nanos(nanos).naive_utc())
        }
        Value::Str(text) => parse_time_text(text),
        _ => CoreReason::DataFormat
            .to_err()
            .with_detail("time field requires RFC3339 text or integer nanoseconds")
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
                    Value::Array(vec![Value::Str("a".into()), Value::Number(2.0)]),
                ),
            ],
            yield_field_types: vec![
                ("count".into(), FieldType::Base(BaseType::Digit)),
                ("items".into(), FieldType::Array(BaseType::Chars)),
            ],
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: String::new(),
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
        assert_eq!(
            record.get_value("items"),
            Some(&ModelValue::from("[\"a\",2.0]"))
        );

        let json = data_record_to_json_string(&record).expect("json");
        assert!(json.contains("\"__wfu_rule_name\":\"rule-a\""));
        assert!(json.contains("\"items\":\"[\\\"a\\\",2.0]\""));
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
            yield_fields: vec![(format!("{WFU_PREFIX}bad"), Value::Str("x".into()))],
            yield_field_types: vec![],
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: String::new(),
        };

        let err = output
            .to_data_record()
            .expect_err("reserved prefix should fail");
        assert!(err.to_string().contains(WFU_PREFIX));
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
            yield_field_types: vec![
                ("src_ip".into(), FieldType::Base(BaseType::Ip)),
                ("seen_at".into(), FieldType::Base(BaseType::Time)),
                ("sha".into(), FieldType::Base(BaseType::Hex)),
            ],
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: String::new(),
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
