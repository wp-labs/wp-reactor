use std::time::Duration;

use winnow::prelude::*;

use super::super::primitives::base_type_parser;
use crate::parse_utils::duration_value;
use crate::schema::{BaseType, FieldType};

// -----------------------------------------------------------------------
// Primitive parsers
// -----------------------------------------------------------------------

#[test]
fn parse_duration_seconds() {
    let d = duration_value.parse("30s").unwrap();
    assert_eq!(d, Duration::from_secs(30));
}

#[test]
fn parse_duration_milliseconds() {
    let d = duration_value.parse("100ms").unwrap();
    assert_eq!(d, Duration::from_millis(100));
}

#[test]
fn parse_duration_minutes() {
    let d = duration_value.parse("5m").unwrap();
    assert_eq!(d, Duration::from_secs(300));
}

#[test]
fn parse_duration_hours() {
    let d = duration_value.parse("48h").unwrap();
    assert_eq!(d, Duration::from_secs(48 * 3600));
}

#[test]
fn parse_duration_days() {
    let d = duration_value.parse("7d").unwrap();
    assert_eq!(d, Duration::from_secs(7 * 86400));
}

#[test]
fn parse_duration_zero() {
    let d = duration_value.parse("0").unwrap();
    assert_eq!(d, Duration::ZERO);
}

#[test]
fn parse_duration_zero_with_suffix() {
    let d = duration_value.parse("0s").unwrap();
    assert_eq!(d, Duration::ZERO);
}

#[test]
fn parse_duration_zero_with_millisecond_suffix() {
    let d = duration_value.parse("0ms").unwrap();
    assert_eq!(d, Duration::ZERO);
}

/// 回归：时长字面量的数字部分是任意 u64 字面量，`num * 倍数` 溢出必须**报错**，
/// 而不是 debug panic / release 静默回绕（旧实现 `num * suffix` 在此 debug panic）。
#[test]
fn parse_duration_overflowing_literal_is_rejected() {
    assert!(duration_value.parse("18446744073709551615d").is_err());
    assert!(duration_value.parse("18446744073709551615h").is_err());
}

/// 边界对照：`u64::MAX / 86400 == 213_503_982_334_601`，恰好不溢出仍应通过。
#[test]
fn parse_duration_day_multiplier_boundary() {
    assert!(duration_value.parse("213503982334601d").is_ok());
    assert!(duration_value.parse("213503982334602d").is_err());
}

/// `s` 后缀倍数为 1，`u64::MAX` 秒本身是合法时长（不参与乘法）。
#[test]
fn parse_duration_max_seconds_is_accepted() {
    let d = duration_value.parse("18446744073709551615s").unwrap();
    assert_eq!(d, Duration::from_secs(u64::MAX));
}

#[test]
fn parse_base_types() {
    assert_eq!(base_type_parser.parse("chars").unwrap(), BaseType::Chars);
    assert_eq!(base_type_parser.parse("digit").unwrap(), BaseType::Digit);
    assert_eq!(base_type_parser.parse("float").unwrap(), BaseType::Float);
    assert_eq!(base_type_parser.parse("bool").unwrap(), BaseType::Bool);
    assert_eq!(base_type_parser.parse("time").unwrap(), BaseType::Time);
    assert_eq!(base_type_parser.parse("ip").unwrap(), BaseType::Ip);
    assert_eq!(base_type_parser.parse("hex").unwrap(), BaseType::Hex);
}

#[test]
fn parse_array_type() {
    let ft = super::super::field_type.parse("array/digit").unwrap();
    assert_eq!(ft, FieldType::Array(BaseType::Digit));
}

#[test]
fn parse_field_decl_simple() {
    let fd = super::super::field_decl.parse("sip: ip").unwrap();
    assert_eq!(fd.name, "sip");
    assert_eq!(fd.field_type, FieldType::Base(BaseType::Ip));
}

#[test]
fn parse_field_decl_dotted() {
    let fd = super::super::field_decl
        .parse("detail.sha256: hex")
        .unwrap();
    assert_eq!(fd.name, "detail.sha256");
    assert_eq!(fd.field_type, FieldType::Base(BaseType::Hex));
}

#[test]
fn parse_field_decl_backtick() {
    let fd = super::super::field_decl.parse("`src-ip`: ip").unwrap();
    assert_eq!(fd.name, "src-ip");
}

#[test]
fn parse_field_decl_array() {
    let fd = super::super::field_decl.parse("tags: array/chars").unwrap();
    assert_eq!(fd.name, "tags");
    assert_eq!(fd.field_type, FieldType::Array(BaseType::Chars));
}
