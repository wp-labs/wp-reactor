//! 时间换算（epoch 归一化等）。
//!
//! epoch 归一的**实现**已全部归并到 `wf-data::time`（2026-09-19）：本模块只剩两个
//! 再导出（保留 `wf-cep` 既有的公开路径与「int / float 双口径」命名）与两个
//! 本域换算函数（`epoch_nanos_to_millis` / `positive_interval_seconds_to_nanos`）。
//!
//! 归并原因：此前 `wf-cep` 与 `wf-data` 各持一份 `normalize_epoch_timestamp_float_nanos`，
//! 且两份行为**不同**（`wf-cep` 已加「整值走 i128 精确乘」的快路，`wf-data` 还是修复前的
//! 「一律 f64 乘 + round」）——而 `wf-data` 那份才在生产路径上（`receiver` 的时间解析：
//! `parse_json_timestamp_nanos` / `parse_timestamp_str_nanos`）。同名两套行为是静默错值的
//! 高危形态，故收敛为单实现。

pub use wf_data::time::normalize_epoch_timestamp_float_nanos;
/// 整数版 epoch 归一化（量级自动判定，阈值与
/// [`normalize_epoch_timestamp_float_nanos`] 一致）。
///
/// 行为与语义见 `wf_data::time::normalize_epoch_timestamp_nanos`：纳秒时间戳超出 f64
/// 精确整数范围，经浮点往返会被量化到 ~256ns，区间界比较会随机翻转。
pub use wf_data::time::normalize_epoch_timestamp_nanos as normalize_epoch_timestamp_int_nanos;

pub fn epoch_nanos_to_millis(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000)
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

    /// 再导出不是「又一份实现」：`wf-cep` 与 `wf-data` 两条路径必须解析到同一个函数。
    /// （归一实现的行为测试集中在 `wf-data::time`，这里只钉住接口不漂移。）
    #[test]
    fn epoch_normalizers_are_reexported_from_wf_data() {
        assert_eq!(
            normalize_epoch_timestamp_float_nanos(1_700_000_000.0),
            wf_data::time::normalize_epoch_timestamp_float_nanos(1_700_000_000.0)
        );
        assert_eq!(
            normalize_epoch_timestamp_int_nanos(1_700_000_000),
            wf_data::time::normalize_epoch_timestamp_nanos(1_700_000_000)
        );
        assert_eq!(
            normalize_epoch_timestamp_int_nanos(1_700_000_000),
            Some(1_700_000_000_000_000_000)
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

    #[test]
    fn epoch_nanos_to_millis_truncates_toward_negative_infinity() {
        assert_eq!(epoch_nanos_to_millis(1_000_000_000), 1_000);
        assert_eq!(epoch_nanos_to_millis(-1), -1);
    }
}
