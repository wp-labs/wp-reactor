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
