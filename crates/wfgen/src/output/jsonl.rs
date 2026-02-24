use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use chrono::{DateTime, Utc};

use crate::datagen::stream_gen::GenEvent;
use crate::oracle::OracleAlert;
use crate::verify::ActualAlert;

/// Write events as JSONL (one JSON object per line).
pub fn write_jsonl(events: &[GenEvent], output_path: &Path) -> anyhow::Result<()> {
    // Create parent directories if needed
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    for event in events {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "_stream".to_string(),
            serde_json::Value::String(event.stream_alias.clone()),
        );
        obj.insert(
            "_window".to_string(),
            serde_json::Value::String(event.window_name.clone()),
        );
        obj.insert(
            "_timestamp".to_string(),
            serde_json::Value::String(event.timestamp.to_rfc3339()),
        );

        // Merge event fields
        for (k, v) in &event.fields {
            obj.insert(k.clone(), v.clone());
        }

        let line = serde_json::to_string(&obj)?;
        writeln!(writer, "{}", line)?;
    }

    writer.flush()?;
    Ok(())
}

/// Write oracle alerts as JSONL.
pub fn write_oracle_jsonl(alerts: &[OracleAlert], output_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    for alert in alerts {
        let line = serde_json::to_string(alert)?;
        writeln!(writer, "{}", line)?;
    }

    writer.flush()?;
    Ok(())
}

/// Read events from a JSONL file.
///
/// Expects each line to contain `_stream`, `_window`, `_timestamp` metadata
/// fields plus the event payload fields.
pub fn read_events_jsonl(path: &Path) -> anyhow::Result<Vec<GenEvent>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&line)?;

        let stream_alias = obj
            .get("_stream")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let window_name = obj
            .get("_window")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let timestamp: DateTime<Utc> = obj
            .get("_timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("1970-01-01T00:00:00Z")
            .parse()
            .unwrap_or_default();

        // Remaining fields (exclude metadata)
        let mut fields = serde_json::Map::new();
        for (k, v) in &obj {
            if !k.starts_with('_') {
                fields.insert(k.clone(), v.clone());
            }
        }

        events.push(GenEvent {
            stream_alias,
            window_name,
            timestamp,
            fields,
        });
    }

    Ok(events)
}

/// Read actual alerts from a JSONL file.
pub fn read_alerts_jsonl(path: &Path) -> anyhow::Result<Vec<ActualAlert>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut alerts = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let alert: ActualAlert = serde_json::from_str(&line)?;
        alerts.push(alert);
    }

    Ok(alerts)
}

/// Read oracle alerts from a JSONL file.
pub fn read_oracle_jsonl(path: &Path) -> anyhow::Result<Vec<OracleAlert>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut alerts = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let alert: OracleAlert = serde_json::from_str(&line)?;
        alerts.push(alert);
    }

    Ok(alerts)
}
