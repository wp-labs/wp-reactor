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

/// Normalize an **integer** epoch timestamp to nanoseconds by digit width.
///
/// 全家族的**整数通道单一实现**（`wf-cep::time::normalize_epoch_timestamp_int_nanos`
/// 已改为再导出本函数，2026-09-19 归并）；单位判定阈值与
/// [`normalize_epoch_timestamp_float_nanos`] 一致。
///
/// 为什么整数与浮点必须是两条通道：纳秒时间戳（≈1.77e18）**超出 f64 的精确整数范围**
/// （2^53≈9.0e15），任何经 `Value::Float(f64)` 的往返都会把它量化到 ~256ns。
/// 区间界比较（`>=` / `<`）在「真值相等或相差 <128ns」时会因此随机翻转——
/// 实测同刻（右行 ts == 区间下界）的跨流配对会丢约一半，且症状是静默的
/// 「规则偶尔不触发」。整数通道全程 i64/i128，无取整。
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

/// Normalize a **floating-point** epoch timestamp to nanoseconds by digit width.
///
/// 全家族的**单一实现**（`wf-cep::time` 已改为再导出本函数，2026-09-19 归并）：
/// 此前 `wf-cep` 与 `wf-data` 各有一份，且 `wf-cep` 那份已加「整值走 i128 精确乘」的快路、
/// 本份还是修复前的「一律 f64 乘 + round」—— 同一个函数名两套行为。
///
/// 为什么要那条快路：整值输入（如秒/毫秒量级）经 `f64 * multiplier` 可能落在
/// f64 非精确区（2^53≈9.0e15 以上，ulp 可达 ~256ns），而同一条纯整数乘（i128）不丢。
/// 非整值输入（如 `"1700000000.123"`）本质无法用 f64 精确表达，仍走浮点。
pub fn normalize_epoch_timestamp_float_nanos(raw: f64) -> Option<i64> {
    if !raw.is_finite() {
        return None;
    }
    let abs = raw.abs();
    let multiplier = epoch_timestamp_unit_multiplier(abs as i64);
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

    // 下面两个（`float_normalizer_accepts_common_units` / `int_normalizer_is_exact_for_epoch_nanos`）
    // 是从 `wf-cep::time` 随实现一起搬过来的：归并后只在 `wf-data` 测一次。

    #[test]
    fn float_normalizer_accepts_common_units() {
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
        // 非有限入参不静默回绕
        assert_eq!(normalize_epoch_timestamp_float_nanos(f64::NAN), None);
        assert_eq!(normalize_epoch_timestamp_float_nanos(f64::INFINITY), None);
    }

    /// 整数通道：epoch-ns 超出 f64 的精确整数范围，浮点往返必丢 ~256ns；整数通道不丢。
    #[test]
    fn int_normalizer_is_exact_for_epoch_nanos() {
        // 非 256 对齐的纳秒时间戳（2026-01-01T00:00:00.000000001Z 附近）
        let ns: i64 = 1_767_225_600_000_000_001;
        assert_eq!(
            normalize_epoch_timestamp_nanos(ns),
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
                normalize_epoch_timestamp_nanos(v),
                normalize_epoch_timestamp_float_nanos(v as f64),
                "v={v} 在 f64 精确区间内，两条通道应一致"
            );
        }
        // 溢出（按秒量级判定 ×1e9 后超 i64）→ None，不静默回绕。
        assert_eq!(normalize_epoch_timestamp_nanos(9_999_999_999), None);
        // 已是纳秒量级（不再乘）→ 原样返回。
        assert_eq!(normalize_epoch_timestamp_nanos(i64::MAX), Some(i64::MAX));
    }

    /// 整值 f64 输入走 i128 精确乘（本次归并带回来的快路）。
    ///
    /// `1_700_000_000_123.0 × 1e6` 的精确积不是本量级 ulp（256）的整数倍，所以
    /// 旧实现（`f64` 乘 + `round`）会给出 `1_700_000_000_123_000_064`——偏 64ns。
    #[test]
    fn float_normalizer_is_exact_for_whole_valued_inputs() {
        for ms in [1_700_000_000_123.0, -1_700_000_000_123.0] {
            let sign = if ms < 0.0 { -1 } else { 1 };
            assert_eq!(
                normalize_epoch_timestamp_float_nanos(ms),
                Some(sign * 1_700_000_000_123_000_000)
            );
            // 纯 f64 路径（旧实现的口径）在这个入参上会丢 64ns —— 快路存在的理由。
            assert_ne!(
                (ms * 1e6f64).round() as i64,
                sign * 1_700_000_000_123_000_000
            );
        }
    }

    /// 单位判定阈值（秒 / 毫秒 / 微秒 / 纳秒四个量级）逐个钉住。
    /// 这是 `abs` 分组边界，改错会静默错算 1000 倍。
    #[test]
    fn int_normalizer_unit_brackets_are_pinned() {
        // 秒量级：×1e9
        assert_eq!(
            normalize_epoch_timestamp_nanos(1_700_000_000),
            Some(1_700_000_000_000_000_000)
        );
        // 毫秒量级：×1e6
        assert_eq!(
            normalize_epoch_timestamp_nanos(10_000_000_000),
            Some(10_000_000_000_000_000)
        );
        // 微秒量级：×1e3
        assert_eq!(
            normalize_epoch_timestamp_nanos(10_000_000_000_000),
            Some(10_000_000_000_000_000)
        );
        // 纳秒量级：×1
        assert_eq!(
            normalize_epoch_timestamp_nanos(1_700_000_000_000_000_000),
            Some(1_700_000_000_000_000_000)
        );
        // 秒量级上界 ×1e9 超出 i64 → None（不静默回绕）
        assert_eq!(normalize_epoch_timestamp_nanos(9_999_999_999), None);
        // 浮点通道共享同一批阈值，必须给出一致结果
        for v in [1_700_000_000i64, 10_000_000_000, 10_000_000_000_000] {
            assert_eq!(
                normalize_epoch_timestamp_nanos(v),
                normalize_epoch_timestamp_float_nanos(v as f64),
                "v={v} 两条通道的量级判定必须一致"
            );
        }
    }

    /// 负值（pre-epoch）与极值不 panic、不误判量级。
    #[test]
    fn int_normalizer_handles_negative_and_extremes() {
        assert_eq!(
            normalize_epoch_timestamp_nanos(-1_700_000_000),
            Some(-1_700_000_000_000_000_000)
        );
        assert_eq!(
            normalize_epoch_timestamp_nanos(-10_000_000_000),
            Some(-10_000_000_000_000_000)
        );
        // i64::MIN 的绝对值无法用 i64 表示 → 视作最高量级（×1），原样返回
        assert_eq!(normalize_epoch_timestamp_nanos(i64::MIN), Some(i64::MIN));
        // 浮点通道对 -0.0 归一为 0（不产生负零纳秒）
        assert_eq!(normalize_epoch_timestamp_float_nanos(-0.0), Some(0));
    }
}
