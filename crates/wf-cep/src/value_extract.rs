//! Arrow 列 → `Value` 值提取核心（P4-B0 下沉）：`extract_field_value` 家族与
//! wfl structured-JSON 字段判定。
//!
//! 行式事件桥（`batch_to_events` / 物化 / 列式视图按需读）与列式求值的值
//! 转换**共用**本模块：Utf8 列带 wfl metadata 时按 structured JSON 解析，
//! 其余按原生列类型转换（Int64/Timestamp(Ns) → `Number` f64 round-trip、
//! Utf8 → `Str`、Boolean → `Bool`、Struct/List 递归）——字节一致性由
//! 对拍测试（columnar_tests / event_bridge_r4 / coverage）锁定。
//! 引擎经 `wf_engine::match_engine::event_bridge` re-export 消费，路径不变。
//! 纯 arrow + serde_json 数据面（墙内允许）；不触 IO/async。

use arrow::array::{
    Array, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray, ListArray,
    StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};

use crate::value::{EngineHashMap, Value};

pub const WFL_FIELD_TYPE_METADATA_KEY: &str = "wf.wfl.field_type";
pub const WFL_FIELD_TYPE_OBJECT: &str = "object";
pub const WFL_FIELD_TYPE_ARRAY: &str = "array";

pub fn is_wfl_structured_field(field: &Field) -> bool {
    wfl_structured_field_kind(field).is_some()
}

pub fn wfl_structured_field_kind(field: &Field) -> Option<&str> {
    // 一次 metadata 查找（旧实现匹配命中后二次 get，纯浪费）。
    let kind = field
        .metadata()
        .get(WFL_FIELD_TYPE_METADATA_KEY)
        .map(String::as_str);
    match kind {
        Some(WFL_FIELD_TYPE_OBJECT | WFL_FIELD_TYPE_ARRAY) => kind,
        _ => None,
    }
}

/// Arrow 列单格 → [`Value`]：行式事件桥（`batch_to_events`）与列式视图按需读
/// 共用，null / 失败提取 → `None`（字段缺席）。Utf8 列先查 wfl metadata：
/// structured JSON 列（object/array）解析成 `Value::Object` / `Value::Array`。
pub fn extract_field_value(field: &Field, col: &dyn Array, row: usize) -> Option<Value> {
    // 先查列类型再查 metadata：只有 Utf8 列才可能是 structured JSON。旧实现先查
    // metadata（每次字段读取的纯开销）——q15 全 Int64 字段每事件 34 次白查，
    // 真实运行热点 wfl_structured_field_kind 312M 次（2026-08-22 实测）。
    if matches!(col.data_type(), DataType::Utf8)
        && let Some(kind) = wfl_structured_field_kind(field)
    {
        let arr = col.as_any().downcast_ref::<StringArray>()?;
        return serde_json::from_str::<serde_json::Value>(arr.value(row))
            .ok()
            .and_then(|value| json_to_structured_value(kind, value));
    }
    extract_value(col, row)
}

/// 列值的**精确整数**读取（Int64 / Timestamp(Ns) 列）。
///
/// [`extract_value`] 把这两类列都转成 `Value::Number(f64)`：纳秒时间戳（≈1.77e18）
/// 超出 f64 精确整数范围（2^53≈9.0e15），会被量化到 ~256ns，使"真值相等/相差 <128ns"
/// 的区间界比较随机翻转（同刻跨流配对丢约一半）。区间界求值因此优先走本函数。
/// 其它列类型（含 structured Utf8）返回 `None`，调用方回落到 `Value` 路径，语义不变。
/// `Value` → 精确 `i64`（**只接受整值**）。
///
/// 物化 `Event` 的字段已经过 `Value::Number(f64)`（纳秒精度已丢），这里只能尽力还原；
/// 真正精确的路径是列式源的 [`extract_field_value_int`]。
pub fn value_to_int(v: Option<&Value>) -> Option<i64> {
    match v? {
        // 精确整数直接命中（无需经 f64 还原）。
        Value::Int(i) => Some(*i),
        Value::Number(n)
            if n.is_finite()
                && n.fract() == 0.0
                && *n >= i64::MIN as f64
                && *n <= i64::MAX as f64 =>
        {
            Some(*n as i64)
        }
        _ => None,
    }
}

pub fn extract_field_value_int(field: &Field, col: &dyn Array, row: usize) -> Option<i64> {
    if col.is_null(row) {
        return None;
    }
    if matches!(col.data_type(), DataType::Utf8) && wfl_structured_field_kind(field).is_some() {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => Some(col.as_any().downcast_ref::<Int64Array>()?.value(row)),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(
            col.as_any()
                .downcast_ref::<TimestampNanosecondArray>()?
                .value(row),
        ),
        _ => None,
    }
}

/// `Value` → `f64`（仅 [`Value::Number`]；其余变体 `None`）。
///
/// 行式路径数值漏斗的**单一实现**：此前 `rows.rs` / `cep/step.rs` /
/// `wf-engine …/stats_exec/eval/keys.rs` 各有一份同体副本，任一处改漏即口径漂移
/// （列式与行式结果不一致会是静默错配）。
pub fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        // 整数域归一：`Int` 与整值 `Number` 走同一数值漏斗（`|i| < 2^53` 精确）。
        Value::Int(i) => Some(*i as f64),
        _ => None,
    }
}

fn extract_value(col: &dyn Array, row: usize) -> Option<Value> {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            Some(Value::Number(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            Some(Value::Number(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(Value::Str(arr.value(row).into()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            Some(Value::Bool(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
            Some(Value::Number(arr.value(row) as f64))
        }
        DataType::Struct(_) => {
            let arr = col.as_any().downcast_ref::<StructArray>()?;
            let mut fields = EngineHashMap::default();
            for (field, child) in arr.fields().iter().zip(arr.columns()) {
                if child.is_null(row) {
                    continue;
                }
                if let Some(value) = extract_value(child.as_ref(), row) {
                    fields.insert(field.name().into(), value);
                }
            }
            Some(Value::Object(fields))
        }
        DataType::List(_) => {
            let arr = col.as_any().downcast_ref::<ListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        DataType::LargeList(_) => {
            let arr = col.as_any().downcast_ref::<LargeListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col.as_any().downcast_ref::<FixedSizeListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        _ => None,
    }
}

/// JSON 标量 → [`Value`]。
///
/// **JSON 数字一律落 [`Value::Number`]，不猜整型**（2026-09-18 决策）：`serde_json` 的
/// 整/浮形态取决于文本写法（`1` 与 `1.0`），不是可靠的类型信号；只有箭头列类型
/// （Int64 / Timestamp(Ns)）这种可靠信号才产出 [`Value::Int`]。
fn json_to_value(value: serde_json::Value) -> Option<Value> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(v) => Some(Value::Bool(v)),
        serde_json::Value::Number(v) => v.as_f64().map(Value::Number),
        serde_json::Value::String(v) => Some(Value::Str(v.into())),
        serde_json::Value::Array(values) => Some(Value::Array(
            values.into_iter().filter_map(json_to_value).collect(),
        )),
        serde_json::Value::Object(fields) => Some(Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| json_to_value(value).map(|value| (key.into(), value)))
                .collect(),
        )),
    }
}

fn json_to_structured_value(kind: &str, value: serde_json::Value) -> Option<Value> {
    match (kind, value) {
        (WFL_FIELD_TYPE_OBJECT, serde_json::Value::Object(fields)) => {
            json_to_value(serde_json::Value::Object(fields))
        }
        (WFL_FIELD_TYPE_ARRAY, serde_json::Value::Array(values)) => {
            json_to_value(serde_json::Value::Array(values))
        }
        _ => None,
    }
}

fn extract_list_values(values: &dyn Array) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            continue;
        }
        if let Some(value) = extract_value(values, idx) {
            out.push(value);
        }
    }
    out
}

#[cfg(test)]
mod int_channel_tests {
    use super::*;
    use arrow::array::TimestampNanosecondArray;

    /// Int64 / Timestamp(Ns) 列走精确通道；`Value` 路径（f64）在同一值上丢精度。
    #[test]
    fn extract_field_value_int_is_exact_for_epoch_nanos() {
        let ns: i64 = 1_767_225_600_000_000_001;
        let ints = Int64Array::from(vec![Some(ns), None, Some(7)]);
        let field = Field::new("t", DataType::Int64, true);
        assert_eq!(
            extract_field_value_int(&field, &ints, 0),
            Some(ns),
            "精确通道"
        );
        assert_eq!(
            extract_field_value_int(&field, &ints, 1),
            None,
            "null → None"
        );

        let via_value = match extract_field_value(&field, &ints, 0) {
            Some(Value::Number(n)) => n as i64,
            other => panic!("expected number, got {other:?}"),
        };
        assert_ne!(via_value, ns, "Value 路径（f64）在这个量级必丢精度");

        let ts = TimestampNanosecondArray::from(vec![Some(ns)]);
        let ts_field = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true);
        assert_eq!(
            extract_field_value_int(&ts_field, &ts, 0),
            Some(ns),
            "Timestamp(Ns) 列同样是精确通道"
        );
    }

    /// 非整数列 / structured Utf8 不参与精确通道（调用方回落 `Value` 路径）。
    #[test]
    fn extract_field_value_int_declines_non_integral_columns() {
        let floats = arrow::array::Float64Array::from(vec![1.5]);
        let field = Field::new("f", DataType::Float64, false);
        assert_eq!(extract_field_value_int(&field, &floats, 0), None);

        let strings = StringArray::from(vec!["x"]);
        let field = Field::new("s", DataType::Utf8, false);
        assert_eq!(extract_field_value_int(&field, &strings, 0), None);
    }

    /// `Value` → i64 只接受整值。
    #[test]
    fn value_to_int_accepts_only_integrals() {
        assert_eq!(value_to_int(Some(&Value::Number(7.0))), Some(7));
        assert_eq!(value_to_int(Some(&Value::Number(7.5))), None);
        assert_eq!(value_to_int(Some(&Value::Str("7".into()))), None);
        assert_eq!(value_to_int(None), None);
    }

    /// 数值漏斗契约：只有 `Number` 产出 `f64`，其它变体（含结构化）一律 `None`——
    /// 列式与行式路径都走本函数，契约漂移会是静默错配。
    #[test]
    fn value_to_f64_only_accepts_numbers() {
        assert_eq!(value_to_f64(&Value::Number(-0.5)), Some(-0.5));
        // 整值也走同一入口（精度由调用方按需走 `extract_field_value_int`）。
        assert_eq!(value_to_f64(&Value::Number(3.0)), Some(3.0));
        assert_eq!(value_to_f64(&Value::Str("1".into())), None);
        assert_eq!(value_to_f64(&Value::Bool(true)), None);
        assert_eq!(value_to_f64(&Value::Array(Vec::new())), None);
        assert_eq!(value_to_f64(&Value::Object(Default::default())), None);
    }

    /// 决策（2026-09-18）：**JSON 数字来源不猜整型**。`serde_json` 的整/浮形态
    /// 取决于文本写法（`1` vs `1.0`），不是可靠信号 —— `json_to_value` 必须永远
    /// 落 [`Value::Number`]，只有箭头列类型（Int64 / Timestamp(Ns)）才产出
    /// [`Value::Int`]。本用例防止后续“顺手”把整值 JSON 猜成 `Int`。
    #[test]
    fn json_numbers_stay_number_never_guessed_as_int() {
        for text in ["1", "1.0", "0", "-0", "9007199254740993", "1e18", "1.5"] {
            let json: serde_json::Value = serde_json::from_str(text).unwrap();
            let got = json_to_value(json).expect("json number → Some");
            assert!(
                matches!(got, Value::Number(_)),
                "JSON `{text}` 必须落 Number，实际 {got:?}"
            );
        }
        // 嵌套形态（数组 / 对象里的整数）同样不猜。
        let nested: serde_json::Value = serde_json::from_str(r#"{"a":[1,2.5]}"#).unwrap();
        let Value::Object(fields) = json_to_value(nested).expect("json object → Some") else {
            panic!("expected object")
        };
        let Value::Array(items) = fields.get("a").expect("字段 a") else {
            panic!("expected array")
        };
        assert!(
            items.iter().all(|v| matches!(v, Value::Number(_))),
            "嵌套元素也不猜: {items:?}"
        );
    }
}
