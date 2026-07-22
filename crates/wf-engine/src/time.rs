pub(crate) fn epoch_nanos_to_millis(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000)
}

pub fn normalize_epoch_timestamp_float_nanos(raw: f64) -> Option<i64> {
    if !raw.is_finite() {
        return None;
    }
    let abs = raw.abs();
    let multiplier = match abs as i64 {
        0..=9_999_999_999 => 1_000_000_000,
        10_000_000_000..=9_999_999_999_999 => 1_000_000,
        10_000_000_000_000..=9_999_999_999_999_999 => 1_000,
        _ => 1,
    };
    if raw.fract() == 0.0 && raw >= i64::MIN as f64 && raw <= i64::MAX as f64 {
        let nanos = i128::from(raw as i64).checked_mul(i128::from(multiplier))?;
        return i64::try_from(nanos).ok();
    }
    let nanos = raw * multiplier as f64;
    if !nanos.is_finite() || nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
        return None;
    }
    Some(nanos.round() as i64)
}

pub(crate) fn positive_interval_seconds_to_nanos(interval_seconds: f64) -> Option<i64> {
    if !interval_seconds.is_finite() || interval_seconds <= 0.0 {
        return None;
    }
    let nanos = interval_seconds * 1_000_000_000.0;
    if !nanos.is_finite() || nanos > i64::MAX as f64 {
        return None;
    }
    let nanos = nanos.round() as i64;
    if nanos > 0 { Some(nanos) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_epoch_timestamp_accepts_common_units() {
        assert_eq!(
            normalize_epoch_timestamp_float_nanos(1_700_000_000.0),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            normalize_epoch_timestamp_float_nanos(1_700_000_000_123.0),
            Some(1_700_000_000_123_000_000)
        );
        assert_eq!(
            normalize_epoch_timestamp_float_nanos(1_700_000_000_123_456.0),
            Some(1_700_000_000_123_456_000)
        );
        assert_eq!(
            normalize_epoch_timestamp_float_nanos(1_700_000_000_123_456_789.0),
            Some(1_700_000_000_123_456_768)
        );
    }

    #[test]
    fn positive_interval_seconds_rejects_invalid_values() {
        assert_eq!(
            positive_interval_seconds_to_nanos(60.0),
            Some(60_000_000_000)
        );
        assert_eq!(positive_interval_seconds_to_nanos(0.0), None);
        assert_eq!(positive_interval_seconds_to_nanos(-1.0), None);
        assert_eq!(positive_interval_seconds_to_nanos(f64::INFINITY), None);
        assert_eq!(positive_interval_seconds_to_nanos(f64::NAN), None);
    }
}
