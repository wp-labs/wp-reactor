use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AlertConfig {
    /// Alert output destinations (URI list, e.g. `"file:///var/log/wf-alerts.jsonl"`).
    pub sinks: Vec<String>,
}

impl AlertConfig {
    /// Parse all sink URIs, returning an error on the first invalid entry.
    pub fn parsed_sinks(&self) -> anyhow::Result<Vec<SinkUri>> {
        self.sinks.iter().map(|s| parse_sink_uri(s)).collect()
    }
}

/// Parsed sink destination.
#[derive(Debug, Clone)]
pub enum SinkUri {
    File { path: PathBuf },
}

/// Parse a raw sink URI string into a [`SinkUri`].
pub fn parse_sink_uri(raw: &str) -> anyhow::Result<SinkUri> {
    if let Some(path_str) = raw.strip_prefix("file://") {
        if path_str.is_empty() {
            anyhow::bail!("file:// sink URI has empty path");
        }
        Ok(SinkUri::File {
            path: PathBuf::from(path_str),
        })
    } else {
        anyhow::bail!("unsupported alert sink URI: {:?} (supported: file://)", raw);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_file_uri() {
        let uri = parse_sink_uri("file:///var/log/alerts.jsonl").unwrap();
        match uri {
            SinkUri::File { path } => {
                assert_eq!(path, PathBuf::from("/var/log/alerts.jsonl"));
            }
        }
    }

    #[test]
    fn reject_empty_file_path() {
        let err = parse_sink_uri("file://").unwrap_err();
        assert!(err.to_string().contains("empty path"));
    }

    #[test]
    fn reject_http_uri() {
        let err = parse_sink_uri("http://localhost:9200").unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn reject_bare_string() {
        let err = parse_sink_uri("/tmp/alerts.jsonl").unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn parsed_sinks_collects() {
        let cfg = AlertConfig {
            sinks: vec!["file:///a.jsonl".to_string(), "file:///b.jsonl".to_string()],
        };
        let uris = cfg.parsed_sinks().unwrap();
        assert_eq!(uris.len(), 2);
    }

    #[test]
    fn parsed_sinks_fails_on_bad_entry() {
        let cfg = AlertConfig {
            sinks: vec!["file:///ok.jsonl".to_string(), "http://bad".to_string()],
        };
        assert!(cfg.parsed_sinks().is_err());
    }
}
