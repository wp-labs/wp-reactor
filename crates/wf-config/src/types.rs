use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// ---------------------------------------------------------------------------
// HumanDuration
// ---------------------------------------------------------------------------

/// A duration parsed from a human-readable string like `"30s"`, `"5m"`, `"1h"`, `"2d"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(Duration);

impl HumanDuration {
    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

impl From<HumanDuration> for Duration {
    fn from(hd: HumanDuration) -> Self {
        hd.0
    }
}

impl From<Duration> for HumanDuration {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}

impl FromStr for HumanDuration {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            anyhow::bail!("empty duration string");
        }

        let (num_part, suffix) = split_number_suffix(s)?;
        let value: u64 = num_part
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid number in duration: {s:?}"))?;

        let secs = match suffix {
            "s" => value,
            "m" => value * 60,
            "h" => value * 3600,
            "d" => value * 86400,
            _ => {
                anyhow::bail!("unsupported duration suffix {suffix:?} in {s:?} (expected s/m/h/d)")
            }
        };

        Ok(Self(Duration::from_secs(secs)))
    }
}

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.0.as_secs();
        if secs == 0 {
            return write!(f, "0s");
        }
        if secs.is_multiple_of(86400) {
            write!(f, "{}d", secs / 86400)
        } else if secs.is_multiple_of(3600) {
            write!(f, "{}h", secs / 3600)
        } else if secs.is_multiple_of(60) {
            write!(f, "{}m", secs / 60)
        } else {
            write!(f, "{secs}s")
        }
    }
}

impl Serialize for HumanDuration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// ByteSize
// ---------------------------------------------------------------------------

/// A byte size parsed from a human-readable string like `"256MB"`, `"2GB"`, `"64KB"`, `"1024B"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(usize);

impl ByteSize {
    pub fn as_bytes(&self) -> usize {
        self.0
    }
}

impl From<ByteSize> for usize {
    fn from(bs: ByteSize) -> Self {
        bs.0
    }
}

impl From<usize> for ByteSize {
    fn from(n: usize) -> Self {
        Self(n)
    }
}

impl FromStr for ByteSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            anyhow::bail!("empty byte-size string");
        }

        // Case-insensitive matching
        let upper = s.to_ascii_uppercase();
        let (num_part, suffix) = split_number_suffix(&upper)?;
        let value: usize = num_part
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid number in byte-size: {s:?}"))?;

        let bytes = match suffix {
            "B" => value,
            "KB" => value * 1024,
            "MB" => value * 1024 * 1024,
            "GB" => value * 1024 * 1024 * 1024,
            _ => anyhow::bail!(
                "unsupported byte-size suffix {suffix:?} in {s:?} (expected B/KB/MB/GB)"
            ),
        };

        Ok(Self(bytes))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.0;
        if b == 0 {
            return write!(f, "0B");
        }
        if b.is_multiple_of(1024 * 1024 * 1024) {
            write!(f, "{}GB", b / (1024 * 1024 * 1024))
        } else if b.is_multiple_of(1024 * 1024) {
            write!(f, "{}MB", b / (1024 * 1024))
        } else if b.is_multiple_of(1024) {
            write!(f, "{}KB", b / 1024)
        } else {
            write!(f, "{b}B")
        }
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// DistMode
// ---------------------------------------------------------------------------

/// Distribution mode for a window. Resolved from flat TOML fields `mode` + `partition_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistMode {
    Local,
    Replicated,
    Partitioned { key: String },
}

// ---------------------------------------------------------------------------
// EvictPolicy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictPolicy {
    TimeFirst,
    Lru,
}

// ---------------------------------------------------------------------------
// LatePolicy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LatePolicy {
    Drop,
    Revise,
    SideOutput,
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Split a string like `"30s"` into `("30", "s")`.
/// Returns an error if the string is all-digits or all-letters.
fn split_number_suffix(s: &str) -> anyhow::Result<(&str, &str)> {
    let idx = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| anyhow::anyhow!("missing suffix in {s:?}"))?;
    if idx == 0 {
        anyhow::bail!("missing numeric part in {s:?}");
    }
    Ok((&s[..idx], &s[idx..]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- HumanDuration --

    #[test]
    fn duration_seconds() {
        let d: HumanDuration = "30s".parse().unwrap();
        assert_eq!(d.as_duration(), Duration::from_secs(30));
        assert_eq!(d.to_string(), "30s");
    }

    #[test]
    fn duration_minutes() {
        let d: HumanDuration = "5m".parse().unwrap();
        assert_eq!(d.as_duration(), Duration::from_secs(300));
        assert_eq!(d.to_string(), "5m");
    }

    #[test]
    fn duration_hours() {
        let d: HumanDuration = "48h".parse().unwrap();
        assert_eq!(d.as_duration(), Duration::from_secs(48 * 3600));
        assert_eq!(d.to_string(), "2d");
    }

    #[test]
    fn duration_days() {
        let d: HumanDuration = "2d".parse().unwrap();
        assert_eq!(d.as_duration(), Duration::from_secs(2 * 86400));
        assert_eq!(d.to_string(), "2d");
    }

    #[test]
    fn duration_zero() {
        let d: HumanDuration = "0s".parse().unwrap();
        assert_eq!(d.as_duration(), Duration::from_secs(0));
        assert_eq!(d.to_string(), "0s");
    }

    #[test]
    fn duration_error_empty() {
        assert!("".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn duration_error_no_suffix() {
        assert!("30".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn duration_error_invalid_suffix() {
        assert!("30x".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn duration_error_no_number() {
        assert!("s".parse::<HumanDuration>().is_err());
    }

    // -- ByteSize --

    #[test]
    fn bytesize_bytes() {
        let b: ByteSize = "1024B".parse().unwrap();
        assert_eq!(b.as_bytes(), 1024);
        assert_eq!(b.to_string(), "1KB");
    }

    #[test]
    fn bytesize_kb() {
        let b: ByteSize = "64KB".parse().unwrap();
        assert_eq!(b.as_bytes(), 64 * 1024);
        assert_eq!(b.to_string(), "64KB");
    }

    #[test]
    fn bytesize_mb() {
        let b: ByteSize = "256MB".parse().unwrap();
        assert_eq!(b.as_bytes(), 256 * 1024 * 1024);
        assert_eq!(b.to_string(), "256MB");
    }

    #[test]
    fn bytesize_gb() {
        let b: ByteSize = "2GB".parse().unwrap();
        assert_eq!(b.as_bytes(), 2 * 1024 * 1024 * 1024);
        assert_eq!(b.to_string(), "2GB");
    }

    #[test]
    fn bytesize_case_insensitive() {
        let b: ByteSize = "256mb".parse().unwrap();
        assert_eq!(b.as_bytes(), 256 * 1024 * 1024);
    }

    #[test]
    fn bytesize_error_empty() {
        assert!("".parse::<ByteSize>().is_err());
    }

    #[test]
    fn bytesize_error_invalid_suffix() {
        assert!("256TB".parse::<ByteSize>().is_err());
    }

    // -- Serde round-trips --

    #[test]
    fn serde_roundtrip_duration() {
        let d: HumanDuration = "30s".parse().unwrap();
        let json = serde_json::to_string(&d).unwrap();
        let d2: HumanDuration = serde_json::from_str(&json).unwrap();
        assert_eq!(d, d2);
    }

    #[test]
    fn serde_roundtrip_bytesize() {
        let b: ByteSize = "256MB".parse().unwrap();
        let json = serde_json::to_string(&b).unwrap();
        let b2: ByteSize = serde_json::from_str(&json).unwrap();
        assert_eq!(b, b2);
    }
}
