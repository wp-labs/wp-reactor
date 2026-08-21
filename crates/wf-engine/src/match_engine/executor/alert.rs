use std::time::{SystemTime, UNIX_EPOCH};
use wf_lang::ast::FieldRef;

use crate::alert::AlertOrigin;
use crate::match_engine::match_engine::{
    StepData, Value, field_ref_name, push_i64_exact_decimal, value_to_string,
};

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

    // Byte-direct write of the fixed "YYYY-MM-DDTHH:MM:SS.mmmZ" layout into a
    // right-sized String — no format! machinery on the per-event hot path.
    // Byte-identical to the previous format!("{:04}-{:02}...") output.
    let mut s = String::with_capacity(24);
    push_digits(&mut s, year as u32, 4);
    s.push('-');
    push_digits(&mut s, month, 2);
    s.push('-');
    push_digits(&mut s, day, 2);
    s.push('T');
    push_digits(&mut s, hour as u32, 2);
    s.push(':');
    push_digits(&mut s, minute as u32, 2);
    s.push(':');
    push_digits(&mut s, second as u32, 2);
    s.push('.');
    push_digits(&mut s, millis, 3);
    s.push('Z');
    s
}

/// Push `v` as `width` zero-padded ASCII decimal digits.
fn push_digits(s: &mut String, v: u32, width: usize) {
    let mut digits = [b'0'; 8];
    let mut i = width;
    let mut v = v;
    while i > 0 {
        i -= 1;
        digits[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    for &d in &digits[..width] {
        s.push(d as char);
    }
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

/// wfx_id 核心：FNV-1a(rule_name \x00 event_time_nanos(LE) \x00 \x00 origin)。
/// 字段不再参与哈希——现实流中同一纳秒不可能有两个事件，全字段渲染+哈希
/// 是 on-each 每行 ~190ns 的大头（微基准 cut A 实测）。字节流与旧实现
/// 「空字段集事件」路径完全一致（字段循环后的分隔符保留）。origin 保留以
/// 区分 event / close 两类告警。
fn wfx_id_from_rule_and_time(
    rule_name: &str,
    event_time_nanos: i64,
    origin: &AlertOrigin,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    hasher.update(&event_time_nanos.to_le_bytes());
    hasher.update(b"\x00");
    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    hex_encode(&hasher.finalize().to_le_bytes())
}

pub(super) fn build_each_wfx_id(
    rule_name: &str,
    event_time_nanos: i64,
    _ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
    _field_order: &[&smol_str::SmolStr],
) -> String {
    wfx_id_from_rule_and_time(rule_name, event_time_nanos, origin)
}

/// [`build_each_wfx_id`] with a caller-provided value-rendering scratch
/// buffer (kept for signature compatibility; rendering no longer allocates).
pub(super) fn build_each_wfx_id_reusing(
    rule_name: &str,
    event_time_nanos: i64,
    _ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
    _field_order: &[&smol_str::SmolStr],
    _scratch: &mut String,
) -> String {
    wfx_id_from_rule_and_time(rule_name, event_time_nanos, origin)
}

/// Batch-constant FNV-1a prefix state for the on-each wfx_id byte stream:
/// the hasher state after `rule_name \x00`. Rule names are constant per rule
/// (tens of bytes on real rule sets) and were previously re-hashed per row;
/// with the prefix hoisted, the per-row suffix is only
/// `time LE \x00 \x00 origin` (~14 bytes).
pub(crate) struct EachWfxPrefix {
    state: u64,
}

impl EachWfxPrefix {
    pub(crate) fn new(rule_name: &str) -> Self {
        let mut hasher = Fnv1a::new();
        hasher.update(rule_name.as_bytes());
        hasher.update(b"\x00");
        Self {
            state: hasher.state,
        }
    }

    /// Per-row finish — byte stream identical to
    /// [`wfx_id_from_rule_and_time`] (locked by unit test).
    pub(crate) fn wfx_id(&self, event_time_nanos: i64, origin: &AlertOrigin) -> String {
        let mut hasher = Fnv1a { state: self.state };
        hasher.update(&event_time_nanos.to_le_bytes());
        hasher.update(b"\x00");
        hasher.update(b"\x00");
        hasher.update(origin.as_str().as_bytes());
        hex_encode(&hasher.finalize().to_le_bytes())
    }
}

/// Append `v` exactly as `value_to_string(&Value::Number(v as f64))` renders
/// it: `|v| <= 2^53` takes the itoa fast path (the exact f64 Display of such
/// an integer is the plain decimal — no ".0", no exponent), larger magnitudes
/// keep the lossy f64 round-trip Display for byte-identity with the eager
/// path. Single source of the 2^53 rendering rule — used by the batch-typed
/// entity column read in the columnar on-each path (locked by
/// `flat_int64_fast_path_matches_f64_roundtrip_bytes`).
pub(crate) fn write_int64_value(scratch: &mut String, v: i64) {
    use std::fmt::Write;
    if v.unsigned_abs() <= (1i64 << 53) as u64 {
        push_i64_exact_decimal(scratch, v);
    } else {
        let _ = write!(scratch, "{}", v as f64);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// Build a human-readable summary.
///
/// Writes directly into a single `String` (no intermediate `Vec<String>` or
/// `join`) — byte-identical to the previous `format!`+`join` implementation,
/// but one allocation per alert instead of one per part plus a final join.
pub(super) fn build_summary(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    use std::fmt::Write as _;

    // Estimate capacity so the common case (a few keys / steps) never reallocates.
    let mut out = String::with_capacity(64 + scope_key.len() * 12 + step_data.len() * 16);
    let _ = write!(out, "rule={}; ", rule_name);

    if scope_key.is_empty() {
        out.push_str("scope=global; ");
    } else {
        out.push_str("scope=[");
        for (i, (fr, val)) in keys.iter().zip(scope_key.iter()).enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{}={}", field_ref_name(fr), value_to_string(val));
        }
        out.push_str("]; ");
    }

    for (i, sd) in step_data.iter().enumerate() {
        match &sd.label {
            Some(label) => {
                let _ = write!(out, "{}={:.1}; ", label, sd.measure_value);
            }
            None => {
                let _ = write!(out, "step{}={:.1}; ", i, sd.measure_value);
            }
        }
    }

    let _ = write!(out, "origin={}", origin.as_str());
    out
}

#[cfg(test)]
mod format_tests {
    use super::*;

    /// Reference implementation: the previous `format!`-based output.
    fn reference(nanos: i64) -> String {
        if nanos <= 0 {
            return "1970-01-01T00:00:00.000Z".to_string();
        }
        let total_secs = (nanos / 1_000_000_000) as u64;
        let millis = ((nanos % 1_000_000_000) / 1_000_000) as u32;
        let secs_of_day = total_secs % 86400;
        let days_since_epoch = (total_secs / 86400) as i64;
        let (year, month, day) = civil_from_days(days_since_epoch);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            year,
            month,
            day,
            secs_of_day / 3600,
            (secs_of_day % 3600) / 60,
            secs_of_day % 60,
            millis
        )
    }

    #[test]
    fn flat_int64_fast_path_matches_f64_roundtrip_bytes() {
        // The 2^53 integer fast path in `write_int64_value` (the single
        // rendering source for Int64 / Timestamp(ns) entity columns on the
        // columnar on-each path) must be byte-identical to the eager
        // `Value::Number(v as f64)` rendering for every value, across the
        // 2^53 exactness boundary.
        let edge_vals: Vec<i64> = vec![
            i64::MIN,
            i64::MIN + 1,
            -(1i64 << 53) - 1,
            -(1i64 << 53),
            -(1i64 << 53) + 1,
            -1,
            0,
            1,
            (1i64 << 53) - 1,
            (1i64 << 53),
            (1i64 << 53) + 1,
            123_456_789,
            999_999_999_999_999_999,
            i64::MAX,
        ];
        for &v in edge_vals.iter() {
            let mut scratch = String::new();
            write_int64_value(&mut scratch, v);
            let expect = value_to_string(&Value::Number(v as f64));
            assert_eq!(scratch, expect, "int64 v={v}");
        }
    }

    #[test]
    fn each_wfx_prefix_matches_scalar_per_row_hash() {
        // The batch-hoisted FNV prefix must produce byte-identical wfx_ids
        // to the per-row scalar hash for every (rule, time) pair.
        let cases: &[(&str, i64)] = &[
            ("q1_bid_passthrough", 1_750_000_000_000_000_000),
            (
                "qradar_rule_with_a_long_name_for_prefix_hoisting_check",
                86_400_000_000_000_000,
            ),
            ("r", 1),
            ("empty_time", 0),
            ("negative", -1),
            ("i64::MAX", i64::MAX),
        ];
        for &(rule, t) in cases {
            assert_eq!(
                EachWfxPrefix::new(rule).wfx_id(t, &AlertOrigin::Event),
                wfx_id_from_rule_and_time(rule, t, &AlertOrigin::Event),
                "rule={rule} t={t}"
            );
        }
    }

    #[test]
    fn format_nanos_utc_matches_reference_on_edges() {
        let edges = [
            0i64,
            -1,
            1,
            999_999,
            1_000_000,
            999_999_999,
            1_000_000_000,
            86_399_999_999_999_999,    // last ms of 1970-01-01
            86_400_000_000_000_000,    // 1970-01-02T00:00:00
            1_583_020_800_000_000_000, // 2020-03-01 (leap year boundary)
            1_609_459_200_000_000_000, // 2021-01-01
            4_102_444_800_000_000_000, // 2100-01-01 (non-leap century)
            i64::MAX,
        ];
        for n in edges {
            assert_eq!(format_nanos_utc(n), reference(n), "edge nanos={n}");
        }
    }

    #[test]
    fn format_nanos_utc_matches_reference_on_pseudorandom_samples() {
        // Deterministic LCG so failures reproduce.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut nanos: i64 = 1;
        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let pick = state % 100;
            let n = if pick < 60 {
                // Recent-ish wall-clock range (2010-2100).
                1_262_304_000_000_000_000 + (state % 2_817_484_800) as i64 * 1_000_000
            } else if pick < 90 {
                // Early epoch.
                (state % 86_400_000_000) as i64
            } else {
                // Full positive i64 span (up to year 2262).
                ((state >> 12) as i64).abs()
            };
            assert_eq!(format_nanos_utc(n), reference(n), "random nanos={n}");
            nanos += 1;
        }
        let _ = nanos;
    }

    #[test]
    fn build_summary_matches_reference() {
        use crate::match_engine::match_engine::EngineHashMap;

        // Reference: the previous `format!` + `Vec<String>` + `join` shape.
        fn reference_summary(
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

        fn step(measure_value: f64, label: Option<&str>) -> StepData {
            StepData {
                satisfied_branch_index: 0,
                label: label.map(|s| s.to_string()),
                measure_value,
                event_first_time_nanos: None,
                event_last_time_nanos: None,
                collected_values: Vec::new(),
                field_values: EngineHashMap::default(),
            }
        }

        let keys = [FieldRef::Simple("auction".to_string())];
        let scope = [Value::Number(421_762.0)];
        let steps = [step(1.0, None), step(2.5, Some("count"))];

        // Empty scope + empty steps.
        assert_eq!(
            build_summary("q22_asof_person", &[], &[], &[], &AlertOrigin::Event),
            reference_summary("q22_asof_person", &[], &[], &[], &AlertOrigin::Event),
        );
        // Populated scope + labelled and unlabelled steps.
        assert_eq!(
            build_summary(
                "q22_asof_person",
                &keys,
                &scope,
                &steps,
                &AlertOrigin::Event
            ),
            reference_summary(
                "q22_asof_person",
                &keys,
                &scope,
                &steps,
                &AlertOrigin::Event
            ),
        );
        // Close origin.
        let origin = AlertOrigin::Close {
            reason: crate::match_engine::CloseReason::Timeout,
        };
        assert_eq!(
            build_summary("q22_asof_person", &keys, &scope, &steps, &origin),
            reference_summary("q22_asof_person", &keys, &scope, &steps, &origin),
        );
    }
}
