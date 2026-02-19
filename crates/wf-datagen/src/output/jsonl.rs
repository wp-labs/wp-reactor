use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::datagen::stream_gen::GenEvent;

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
