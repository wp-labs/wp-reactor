use std::sync::atomic::{AtomicU64, Ordering};

use wf_lang::ast::FieldRef;

use crate::rule::match_engine::{field_ref_name, value_to_string, StepData, Value};

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

/// Process-wide monotonic counter for alert_id uniqueness.
static ALERT_SEQ: AtomicU64 = AtomicU64::new(0);

/// Percent-encode characters that would break alert_id structure.
///
/// Encodes `%`, `|`, `#`, and `\x1f` so the three-segment `|` split and
/// the `#seq` suffix can always be parsed unambiguously.
fn encode_alert_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '|' => out.push_str("%7C"),
            '#' => out.push_str("%23"),
            '\x1f' => out.push_str("%1F"),
            _ => out.push(ch),
        }
    }
    out
}

/// Build a composite alert id: `"rule|key1\x1fkey2|fired_at#seq"`.
///
/// - Each key value is percent-encoded (`|` → `%7C`, `#` → `%23`, `%` → `%25`)
///   so that the outer `|` split is always unambiguous.
/// - Keys joined with `\x1f` (unit separator) to avoid multi-key ambiguity.
/// - `seq` is a process-wide monotonic counter for same-millisecond uniqueness.
pub(super) fn build_alert_id(rule_name: &str, scope_key: &[Value], fired_at: &str) -> String {
    let rule_enc = encode_alert_segment(rule_name);
    let keys_part = if scope_key.is_empty() {
        "global".to_string()
    } else {
        scope_key
            .iter()
            .map(|v| encode_alert_segment(&value_to_string(v)))
            .collect::<Vec<_>>()
            .join("\x1f")
    };
    let seq = ALERT_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{}|{}|{}#{}", rule_enc, keys_part, fired_at, seq)
}

/// Build a human-readable summary.
pub(super) fn build_summary(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    close_reason: Option<&str>,
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

    if let Some(reason) = close_reason {
        parts.push(format!("close_reason={}", reason));
    }

    parts.join("; ")
}
