use std::collections::HashMap;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::rngs::StdRng;
use serde_json::Value;
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::wsc_ast::{GenExpr, StreamBlock};

use super::field_gen::generate_field_value;

/// A single generated event.
#[derive(Debug, Clone)]
pub struct GenEvent {
    pub stream_alias: String,
    pub window_name: String,
    pub timestamp: DateTime<Utc>,
    pub fields: serde_json::Map<String, Value>,
}

/// Generate events for a single stream.
pub fn generate_stream_events(
    stream: &StreamBlock,
    schema: &WindowSchema,
    event_count: u64,
    start: &DateTime<Utc>,
    duration: &std::time::Duration,
    rng: &mut StdRng,
) -> Vec<GenEvent> {
    let mut events = Vec::with_capacity(event_count as usize);

    // Build field override lookup
    let overrides: HashMap<&str, &GenExpr> = stream
        .overrides
        .iter()
        .map(|o| (o.field_name.as_str(), &o.gen_expr))
        .collect();

    let duration_nanos = duration.as_nanos() as i64;
    let interval = if event_count > 1 {
        duration_nanos / (event_count as i64)
    } else {
        0
    };

    for i in 0..event_count {
        let ts = *start + ChronoDuration::nanoseconds(interval * i as i64);

        let mut fields = serde_json::Map::new();

        for field_def in &schema.fields {
            let override_expr = overrides.get(field_def.name.as_str()).copied();

            // For Time fields, set the timestamp
            if matches!(&field_def.field_type, FieldType::Base(BaseType::Time)) {
                if override_expr.is_none()
                    || matches!(override_expr, Some(GenExpr::GenFunc { name, .. }) if name == "timestamp")
                {
                    fields.insert(field_def.name.clone(), Value::String(ts.to_rfc3339()));
                    continue;
                }
            }

            let value = generate_field_value(&field_def.field_type, override_expr, rng);
            fields.insert(field_def.name.clone(), value);
        }

        events.push(GenEvent {
            stream_alias: stream.alias.clone(),
            window_name: stream.window.clone(),
            timestamp: ts,
            fields,
        });
    }

    events
}
