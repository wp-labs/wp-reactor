use std::time::{SystemTime, UNIX_EPOCH};
use wf_lang::ast::FieldRef;

use crate::alert::AlertOrigin;
use crate::match_engine::match_engine::{StepData, Value, field_ref_name, value_to_string};

/// Format nanoseconds since epoch as ISO 8601 UTC string.
///
/// Reuses the Hinnant civil-from-days algorithm. For `nanos <= 0`
/// returns the epoch string.
pub(crate) fn format_nanos_utc(nanos: i64) -> String {
    if nanos <= 0 {
        return "1970-01-01T00:00:00.000Z".to_string();
    }
    let total_secs = (nanos / 1_000_000_000) as u64;
    let millis = ((nanos % 1_000_000_000) / 1_000_000) as u32;

    let secs_of_day = total_secs % 86400;
    let days_since_epoch = (total_secs / 86400) as i64;

    let (year, month, day) = civil_from_days(days_since_epoch);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hour, minute, second, millis
    )
}

pub(crate) fn now_nanos() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

/// Hinnant civil_from_days: convert days since 1970-01-01 to (y, m, d).
/// Reference: <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// FNV-1a 64-bit — cheap content hash for the per-alert output ID. The ID only
/// needs to be stable + collision-resistant enough for record identity; a
/// cryptographic hash here is pure overhead on high-throughput alert paths
/// (wfusion: 每 match 一次 SHA-256 曾是执行路径的最大单点)。
struct Fnv1a {
    state: u64,
}

impl Fnv1a {
    fn new() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state ^= b as u64;
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finalize(self) -> u64 {
        self.state
    }
}

/// Build a content-addressed output ID (16 hex chars).
///
/// Feeds rule_name, scope_key, fired_at, step_data, and origin
/// through FNV-1a, then hex-encodes the 8-byte hash as 16 hex characters.
pub(super) fn build_wfx_id(
    rule_name: &str,
    scope_key: &[Value],
    fired_at: &str,
    step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    for v in scope_key {
        hasher.update(value_to_string(v).as_bytes());
        hasher.update(b"\x1f");
    }
    hasher.update(b"\x00");
    hasher.update(fired_at.as_bytes());
    hasher.update(b"\x00");
    for sd in step_data {
        if let Some(label) = &sd.label {
            hasher.update(label.as_bytes());
        }
        hasher.update(b"\x1e");
        hasher.update(&sd.measure_value.to_bits().to_le_bytes());
        hasher.update(b"\x1f");
    }
    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    let hash = hasher.finalize();
    // 8 bytes → 16 hex characters
    hex_encode(&hash.to_le_bytes())
}

pub(super) fn build_each_wfx_id(
    rule_name: &str,
    event_time_nanos: i64,
    ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    hasher.update(&event_time_nanos.to_le_bytes());
    hasher.update(b"\x00");

    let mut fields: Vec<_> = ctx.fields.iter().collect();
    fields.sort_by_key(|(name, _)| *name);
    for (name, value) in fields {
        hasher.update(name.as_bytes());
        hasher.update(b"\x1e");
        hasher.update(value_to_string(value).as_bytes());
        hasher.update(b"\x1f");
    }

    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    let hash = hasher.finalize();
    hex_encode(&hash.to_le_bytes())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Build a human-readable summary.
pub(super) fn build_summary(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    let mut parts = Vec::new();

    parts.push(format!("rule={}", rule_name));

    if scope_key.is_empty() {
        parts.push("scope=global".to_string());
    } else {
        let key_strs: Vec<String> = keys
            .iter()
            .zip(scope_key.iter())
            .map(|(fr, val)| format!("{}={}", field_ref_name(fr), value_to_string(val)))
            .collect();
        parts.push(format!("scope=[{}]", key_strs.join(", ")));
    }

    for (i, sd) in step_data.iter().enumerate() {
        let label_part = match &sd.label {
            Some(l) => format!("{}={:.1}", l, sd.measure_value),
            None => format!("step{}={:.1}", i, sd.measure_value),
        };
        parts.push(label_part);
    }

    parts.push(format!("origin={}", origin.as_str()));

    parts.join("; ")
}
