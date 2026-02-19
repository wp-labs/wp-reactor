use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;

use super::AlertRecord;

/// Trait for alert output destinations.
pub trait AlertSink: Send + Sync {
    fn send(&self, record: &AlertRecord) -> Result<()>;
}

/// Appends alerts as JSON Lines to a file.
pub struct FileAlertSink {
    writer: Mutex<BufWriter<File>>,
}

impl FileAlertSink {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

impl AlertSink for FileAlertSink {
    fn send(&self, record: &AlertRecord) -> Result<()> {
        let json = serde_json::to_string(record)?;
        let mut w = self.writer.lock().expect("alert sink lock poisoned");
        w.write_all(json.as_bytes())?;
        w.write_all(b"\n")?;
        w.flush()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn sample_alert() -> AlertRecord {
        AlertRecord {
            alert_id: "test_rule|192.168.1.1|2024-01-01T00:00:00.000Z#0".to_string(),
            rule_name: "test_rule".to_string(),
            score: 75.0,
            entity_type: "ip".to_string(),
            entity_id: "192.168.1.1".to_string(),
            close_reason: None,
            fired_at: "2024-01-01T00:00:00.000Z".to_string(),
            matched_rows: vec![],
            summary: "rule=test_rule; scope=[sip=192.168.1.1]".to_string(),
        }
    }

    #[test]
    fn test_alert_record_serialization() {
        let alert = sample_alert();
        let json = serde_json::to_string(&alert).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["rule_name"], "test_rule");
        assert_eq!(parsed["score"], 75.0);
        assert_eq!(parsed["entity_type"], "ip");
        assert_eq!(parsed["entity_id"], "192.168.1.1");
        assert!(parsed["close_reason"].is_null());
        // matched_rows should be skipped
        assert!(parsed.get("matched_rows").is_none());
    }

    #[test]
    fn test_file_alert_sink_writes_jsonl() {
        let dir = std::env::temp_dir().join("wf_test_alert_sink");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("alerts.jsonl");
        let _ = std::fs::remove_file(&path);

        {
            let sink = FileAlertSink::open(&path).unwrap();
            sink.send(&sample_alert()).unwrap();

            let mut alert2 = sample_alert();
            alert2.rule_name = "rule_two".to_string();
            alert2.close_reason = Some("timeout".to_string());
            sink.send(&alert2).unwrap();
        }

        let mut contents = String::new();
        File::open(&path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        let lines: Vec<&str> = contents.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["rule_name"], "test_rule");

        let parsed2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed2["rule_name"], "rule_two");
        assert_eq!(parsed2["close_reason"], "timeout");

        // Cleanup
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
