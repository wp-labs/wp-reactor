use chrono::{DateTime, NaiveDateTime};

/// Parse a timestamp-like JSON value into internal nanoseconds since epoch.
///
/// Numeric epoch values are recognized by digit width:
/// seconds, milliseconds, microseconds, then nanoseconds. Strings may be
/// RFC3339, `%Y-%m-%d %H:%M:%S`, or numeric epoch values using the same unit
/// inference.
pub fn parse_json_timestamp_nanos(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => parse_json_number_timestamp_nanos(number),
        serde_json::Value::String(text) => parse_timestamp_str_nanos(text),
        _ => None,
    }
}

/// Parse a timestamp-like string into internal nanoseconds since epoch.
pub fn parse_timestamp_str_nanos(text: &str) -> Option<i64> {
    if let Some(nanos) = DateTime::parse_from_rfc3339(text)
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| dt.and_utc().fixed_offset())
        })
        .and_then(|dt| dt.timestamp_nanos_opt())
    {
        return Some(nanos);
    }
    if let Ok(raw) = text.parse::<i64>() {
        return normalize_epoch_timestamp_nanos(raw);
    }
    text.parse::<f64>()
        .ok()
        .and_then(normalize_epoch_timestamp_float_nanos)
}

/// Normalize an integer epoch timestamp to nanoseconds by digit width.
pub fn normalize_epoch_timestamp_nanos(raw: i64) -> Option<i64> {
    let abs = raw.checked_abs().unwrap_or(i64::MAX);
    let multiplier = epoch_timestamp_unit_multiplier(abs);
    let nanos = i128::from(raw).checked_mul(i128::from(multiplier))?;
    i64::try_from(nanos).ok()
}

fn parse_json_number_timestamp_nanos(number: &serde_json::Number) -> Option<i64> {
    if let Some(raw) = number.as_i64() {
        return normalize_epoch_timestamp_nanos(raw);
    }
    number
        .as_f64()
        .and_then(normalize_epoch_timestamp_float_nanos)
}

fn normalize_epoch_timestamp_float_nanos(raw: f64) -> Option<i64> {
    if !raw.is_finite() {
        return None;
    }
    let abs = raw.abs();
    let multiplier = epoch_timestamp_unit_multiplier(abs as i64);
    let nanos = raw * multiplier as f64;
    if !nanos.is_finite() || nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
        return None;
    }
    Some(nanos.round() as i64)
}

fn epoch_timestamp_unit_multiplier(abs: i64) -> i64 {
    match abs {
        0..=9_999_999_999 => 1_000_000_000,
        10_000_000_000..=9_999_999_999_999 => 1_000_000,
        10_000_000_000_000..=9_999_999_999_999_999 => 1_000,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: serde_json::Value) -> i64 {
        parse_json_timestamp_nanos(&value).expect("timestamp should parse")
    }

    #[test]
    fn parses_epoch_timestamps_by_digit_width() {
        assert_eq!(
            parse(serde_json::json!(1_759_878_077)),
            1_759_878_077_000_000_000
        );
        assert_eq!(
            parse(serde_json::json!(1_759_878_077_000i64)),
            1_759_878_077_000_000_000
        );
        assert_eq!(
            parse(serde_json::json!(1_759_878_077_000_000i64)),
            1_759_878_077_000_000_000
        );
        assert_eq!(
            parse(serde_json::json!(1_759_878_077_000_000_000i64)),
            1_759_878_077_000_000_000
        );
    }

    #[test]
    fn parses_timestamp_strings() {
        assert_eq!(
            parse(serde_json::json!("1759878077000")),
            1_759_878_077_000_000_000
        );
        assert_eq!(
            parse(serde_json::json!("2025-10-07T21:41:17Z")),
            1_759_873_277_000_000_000
        );
        assert_eq!(
            parse(serde_json::json!("2023-11-14 22:13:20")),
            1_700_000_000_000_000_000
        );
    }
}
