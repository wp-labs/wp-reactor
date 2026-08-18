use std::time::{SystemTime, UNIX_EPOCH};
use wf_lang::ast::FieldRef;

use arrow::array::Array;
use arrow::datatypes::{DataType, TimeUnit};

use crate::alert::AlertOrigin;
use crate::match_engine::event_bridge::extract_field_value;
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

pub(super) fn build_each_wfx_id(
    rule_name: &str,
    event_time_nanos: i64,
    ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
    field_order: &[&smol_str::SmolStr],
) -> String {
    let mut scratch = String::new();
    build_each_wfx_id_reusing(
        rule_name,
        event_time_nanos,
        ctx,
        origin,
        field_order,
        &mut scratch,
    )
}

/// [`build_each_wfx_id`] with a caller-provided value-rendering scratch
/// buffer. The batched on-each direct path reuses one buffer across a whole
/// event batch — `clear()` keeps the capacity, so per-row rendering stops
/// re-allocating. The hashed byte stream is identical to the allocating
/// version.
pub(super) fn build_each_wfx_id_reusing(
    rule_name: &str,
    event_time_nanos: i64,
    ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
    field_order: &[&smol_str::SmolStr],
    scratch: &mut String,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    hasher.update(&event_time_nanos.to_le_bytes());
    hasher.update(b"\x00");

    // Field values are hashed through their `value_to_string` rendering, but
    // written into one scratch `String` reused across fields instead of one
    // heap allocation per field (the byte stream is identical — wfx_id values
    // are stable against the pre-optimization path).
    let ordered = field_order.len() == ctx.fields.len();
    if ordered {
        // Schema order precomputed once per batch (same window → same
        // columns): skip the per-event collect + sort entirely.
        for name in field_order {
            if let Some(value) = ctx.fields.get(*name) {
                hasher.update(name.as_bytes());
                hasher.update(b"\x1e");
                write_value_scratch(value, scratch);
                hasher.update(scratch.as_bytes());
                hasher.update(b"\x1f");
            }
        }
    } else {
        // No order supplied (single-event call sites / schema drift within a
        // batch) — fall back to the original per-event collect + sort.
        let mut fields: Vec<_> = ctx.fields.iter().collect();
        fields.sort_by_key(|(name, _)| *name);
        for (name, value) in fields {
            hasher.update(name.as_bytes());
            hasher.update(b"\x1e");
            write_value_scratch(value, scratch);
            hasher.update(scratch.as_bytes());
            hasher.update(b"\x1f");
        }
    }

    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    let hash = hasher.finalize();
    hex_encode(&hash.to_le_bytes())
}

/// Columnar twin of [`build_each_wfx_id_reusing`] over a [`ColumnarEvent`]
/// (no per-row `Event` materialization). The hashed byte stream is identical:
/// field set/order and `value_to_string` renderings match the eager path
/// exactly — flat columns render straight from the Arrow column (no per-row
/// Value build / string clone), structured/other columns go through the full
/// extraction (absent on failure, mirroring `Event.fields`).
pub(crate) fn build_each_wfx_id_columnar_reusing(
    rule_name: &str,
    event_time_nanos: i64,
    event: &crate::match_engine::event_bridge::ColumnarEvent<'_>,
    sorted_fields: &[(String, usize)],
    origin: &AlertOrigin,
    scratch: &mut String,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    hasher.update(&event_time_nanos.to_le_bytes());
    hasher.update(b"\x00");

    // Name-sorted field list is hoisted once per batch; iterate it per row,
    // skipping nulls (absent fields, exactly like `Event.fields`). The eager
    // deferred path passes an empty field_order, so it always hashes the
    // per-row sorted branch — this iteration is that branch, byte-identical.
    for (name, idx) in sorted_fields {
        let schema = event.batch().schema();
        let col = event.batch().column(*idx);
        if col.is_null(event.row()) {
            continue;
        }
        let field = schema.field(*idx);
        let structured =
            crate::match_engine::event_bridge::wfl_structured_field_kind(field).is_some();
        let flat = matches!(
            col.data_type(),
            DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Boolean
                | DataType::Timestamp(_, _)
        ) && !structured;
        if !flat {
            // Structured / nested / unusual columns: full extraction (absent
            // on failure — same field set as the eager path).
            let Some(value) = extract_field_value(field, col.as_ref(), event.row()) else {
                continue;
            };
            hasher.update(name.as_bytes());
            hasher.update(b"\x1e");
            write_value_scratch(&value, scratch);
            hasher.update(scratch.as_bytes());
            hasher.update(b"\x1f");
            continue;
        }
        hasher.update(name.as_bytes());
        hasher.update(b"\x1e");
        write_flat_column_scratch(col.as_ref(), event.row(), scratch);
        hasher.update(scratch.as_bytes());
        hasher.update(b"\x1f");
    }

    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    let hash = hasher.finalize();
    hex_encode(&hash.to_le_bytes())
}

/// Append `v` as a plain decimal integer — the exact rendering of
/// `(v as f64).to_string()` when `|v| <= 2^53` (the fast path in
/// [`write_flat_column_scratch`]), without the `fmt` machinery (~3-5× cheaper
/// than `write!(scratch, "{v}")` on this hot path).
fn write_i64_exact_decimal(scratch: &mut String, mut v: i64) {
    if v == i64::MIN {
        scratch.push_str("-9223372036854775808");
        return;
    }
    if v < 0 {
        scratch.push('-');
        v = -v;
    }
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    // SAFETY: `buf[i..]` contains only ASCII digits ('0'..='9').
    let digits = std::str::from_utf8(&buf[i..]).expect("decimal digits are ASCII");
    scratch.push_str(digits);
}

/// Render a flat column value into `scratch`, byte-identical to
/// `value_to_string(extract_value(...))` but without building a [`Value`]:
/// Int64/Timestamp go through the f64 round-trip (`as f64` → Display) exactly
/// like `Value::Number(i as f64)`, Utf8 pushes the string directly.
fn write_flat_column_scratch(col: &dyn Array, row: usize, scratch: &mut String) {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use std::fmt::Write;
    scratch.clear();
    match col.data_type() {
        DataType::Int64 => {
            let v = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row))
                .unwrap_or(0);
            if v.unsigned_abs() <= (1i64 << 53) as u64 {
                // Fast path: |v| <= 2^53 rounds to an exact f64 whose Display
                // is the plain decimal integer (no ".0", no exponent) —
                // byte-identical to the `v as f64` rendering below, but much
                // cheaper. Outside 2^53 the round-trip may be lossy — keep
                // the f64 path.
                write_i64_exact_decimal(scratch, v);
            } else {
                let _ = write!(scratch, "{}", v as f64);
            }
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row))
                .unwrap_or(0.0);
            let _ = write!(scratch, "{v}");
        }
        DataType::Utf8 => {
            let s = col
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| a.value(row))
                .unwrap_or("");
            scratch.push_str(s);
        }
        DataType::Boolean => {
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|a| a.value(row))
                .unwrap_or(false);
            let _ = write!(scratch, "{v}");
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            use arrow::array::TimestampNanosecondArray;
            let v = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .map(|a| a.value(row))
                .unwrap_or(0);
            if v.unsigned_abs() <= (1i64 << 53) as u64 {
                write_i64_exact_decimal(scratch, v);
            } else {
                let _ = write!(scratch, "{}", v as f64);
            }
        }
        _ => {
            // Unreachable for the `flat` gate; defensive fallback.
            scratch.push_str("[array]");
        }
    }
}

/// Render a [`Value`] into `out` byte-identically to `value_to_string`, but
/// reusing the caller's buffer (clear + rewrite) instead of allocating.
fn write_value_scratch(v: &Value, out: &mut String) {
    out.clear();
    match v {
        Value::Number(n) => {
            use std::fmt::Write;
            let _ = write!(out, "{n}");
        }
        Value::Str(s) => out.push_str(s),
        Value::Bool(b) => {
            use std::fmt::Write;
            let _ = write!(out, "{b}");
        }
        Value::Array(_) => out.push_str("[array]"),
        Value::Object(_) => out.push_str("[object]"),
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
        // The 2^53 integer fast path in `write_flat_column_scratch` must be
        // byte-identical to the eager `Value::Number(v as f64)` rendering for
        // every Int64/Timestamp value, across the 2^53 exactness boundary.
        use arrow::array::{Int64Array, TimestampNanosecondArray};
        use arrow::datatypes::TimeUnit;
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
        let ints = Int64Array::from(edge_vals.clone());
        for (row, &v) in edge_vals.iter().enumerate() {
            let mut scratch = String::new();
            write_flat_column_scratch(&ints, row, &mut scratch);
            let expect = value_to_string(&Value::Number(v as f64));
            assert_eq!(scratch, expect, "int64 v={v}");
        }
        let ts = TimestampNanosecondArray::from(edge_vals.clone());
        assert_eq!(
            ts.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        for (row, &v) in edge_vals.iter().enumerate() {
            let mut scratch = String::new();
            write_flat_column_scratch(&ts, row, &mut scratch);
            let expect = value_to_string(&Value::Number(v as f64));
            assert_eq!(scratch, expect, "timestamp v={v}");
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
}
