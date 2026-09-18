pub fn epoch_nanos_to_millis(nanos: i64) -> i64 {
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

/// 整数版的 epoch 时间戳归一化（单位按量级自动判定，阈值与
/// [`normalize_epoch_timestamp_float_nanos`] 一致）。
///
/// 供**区间界**求值用：纳秒时间戳（≈1.77e18）**超出 f64 的精确整数范围**
/// （2^53≈9.0e15），任何经 `Value::Number(f64)` 的往返都会把它量化到 ~256ns。
/// 区间界比较（`>=` / `<`）在「真值相等或相差 <128ns」时会因此随机翻转——
/// 实测同刻（右行 ts == 区间下界）的跨流配对会丢约一半，且症状是静默的
/// "规则偶尔不触发"。整数通道全程 i64/i128，无取整。
pub fn normalize_epoch_timestamp_int_nanos(raw: i64) -> Option<i64> {
    let abs = raw.unsigned_abs();
    let multiplier: i64 = if abs <= 9_999_999_999 {
        1_000_000_000
    } else if abs <= 9_999_999_999_999 {
        1_000_000
    } else if abs <= 9_999_999_999_999_999 {
        1_000
    } else {
        1
    };
    i64::try_from(i128::from(raw).checked_mul(i128::from(multiplier))?).ok()
}

pub fn positive_interval_seconds_to_nanos(interval_seconds: f64) -> Option<i64> {
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

    /// 整数通道：epoch-ns 超出 f64 的精确整数范围，浮点往返必丢 ~256ns；整数通道不丢。
    #[test]
    fn int_normalizer_is_exact_for_epoch_nanos() {
        // 非 256 对齐的纳秒时间戳（2026-01-01T00:00:00.000000001Z 附近）
        let ns: i64 = 1_767_225_600_000_000_001;
        assert_eq!(
            normalize_epoch_timestamp_int_nanos(ns),
            Some(ns),
            "整数通道必须精确"
        );
        assert_ne!(
            normalize_epoch_timestamp_float_nanos(ns as f64),
            Some(ns),
            "浮点往返在这个量级必然丢精度——精确通道存在的理由（回归锁定）"
        );
        // 秒 / 毫秒 / 微秒量级落在 f64 精确区间内，两条通道必须一致。
        for v in [1_767_225_600i64, 1_767_225_600_000, 1_767_225_600_000_000] {
            assert_eq!(
                normalize_epoch_timestamp_int_nanos(v),
                normalize_epoch_timestamp_float_nanos(v as f64),
                "v={v} 在 f64 精确区间内，两条通道应一致"
            );
        }
        // 溢出（按秒量级判定 ×1e9 后超 i64）→ None，不静默回绕。
        assert_eq!(normalize_epoch_timestamp_int_nanos(9_999_999_999), None);
        // 已是纳秒量级（不再乘）→ 原样返回。
        assert_eq!(
            normalize_epoch_timestamp_int_nanos(i64::MAX),
            Some(i64::MAX)
        );
    }
}
