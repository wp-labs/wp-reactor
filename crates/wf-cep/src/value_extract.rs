//! Arrow 列 → `Value` 值提取核心（P4-B0 下沉）：`extract_field_value` 家族与
//! wfl structured-JSON 字段判定。
//!
//! 行式事件桥（`batch_to_events` / 物化 / 列式视图按需读）与列式求值的值
//! 转换**共用**本模块：Utf8 列带 wfl metadata 时按 structured JSON 解析，
//! 其余按原生列类型转换（Int64/Timestamp(Ns) → `Float` f64 round-trip、
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
/// 共用。Utf8 列先查 wfl metadata：structured JSON 列（object/array）解析成
/// `Value::Object` / `Value::Array`。
///
/// **前置条件：调用方必须先过滤 null**（`col.is_null(row)` → 字段缺席）。本函数
/// 位于字段读取热路径（q15 每事件 ~34 次），因此不在内部重复位图查询——所有调用点
/// （`batch_to_events` / `materialize_rows` / `ColumnarEvent::value_at` /
/// `JoinRow::field_value`）都已先查。不支持的列类型 / JSON 解析失败 → `None`。
///
/// 类型映射：[`Int64`](arrow::datatypes::DataType::Int64) 与
/// `Timestamp(Ns)` → [`Value::Int`]（精确，不经 f64）；`Float64` → `Float`。
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

/// `Value` → `f64`（[`Value::Float`] 与 [`Value::Int`]；其余变体 `None`）。
///
/// 行式路径数值漏斗的**单一实现**：此前 `rows.rs` / `cep/step.rs` /
/// `wf-engine …/stats_exec/eval/keys.rs` 各有一份同体副本，任一处改漏即口径漂移
/// （列式与行式结果不一致会是静默错配）。
pub fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float(n) => Some(*n),
        // 整数域归一：`Int` 与整值 `Float` 走同一数值漏斗（`|i| < 2^53` 精确）。
        Value::Int(i) => Some(*i as f64),
        _ => None,
    }
}

fn extract_value(col: &dyn Array, row: usize) -> Option<Value> {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            // 精确整数（>2^53 不经 f64 量化）—— 见 `Value::Int` 文档。
            Some(Value::Int(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            Some(Value::Float(arr.value(row)))
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
            // epoch-ns（≈1.77e18 > 2^53）必须走精确整数，否则量化到 ~256ns。
            Some(Value::Int(arr.value(row)))
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
/// **JSON 数字一律落 [`Value::Float`]，不猜整型**（2026-09-18 决策）：`serde_json` 的
/// 整/浮形态取决于文本写法（`1` 与 `1.0`），不是可靠的类型信号；只有箭头列类型
/// （Int64 / Timestamp(Ns)）这种可靠信号才产出 [`Value::Int`]。
fn json_to_value(value: serde_json::Value) -> Option<Value> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(v) => Some(Value::Bool(v)),
        serde_json::Value::Number(v) => v.as_f64().map(Value::Float),
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

    /// Int64 / Timestamp(Ns) 列经 `Value` 路径**逐位精确**（`Value::Int`），
    /// 不再有独立的精确读取通道；其余列类型仍按各自语义。
    #[test]
    fn extract_field_value_is_exact_for_int_columns() {
        let ns: i64 = 1_767_225_600_000_000_001;
        assert_ne!(ns as f64 as i64, ns, "前提：该值经 f64 必丢精度");

        let ints = Int64Array::from(vec![Some(ns), None, Some(7)]);
        let field = Field::new("t", DataType::Int64, true);
        assert_eq!(extract_field_value(&field, &ints, 0), Some(Value::Int(ns)));
        assert_eq!(extract_field_value(&field, &ints, 2), Some(Value::Int(7)));
        // null 的过滤是**调用方**职责（见 `extract_field_value` 的前置条件）：
        // 列式视图与事件桥都在调用前 `is_null` 判断，这里不重复断言。

        let ts = TimestampNanosecondArray::from(vec![Some(ns)]);
        let ts_field = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true);
        assert_eq!(extract_field_value(&ts_field, &ts, 0), Some(Value::Int(ns)));

        // 浮点列仍是 `Float`（不猜整型）。
        let floats = arrow::array::Float64Array::from(vec![1.5]);
        let f_field = Field::new("f", DataType::Float64, false);
        assert_eq!(
            extract_field_value(&f_field, &floats, 0),
            Some(Value::Float(1.5))
        );
    }

    /// 数值漏斗契约：只有 `Float` 产出 `f64`，其它变体（含结构化）一律 `None`——
    /// 列式与行式路径都走本函数，契约漂移会是静默错配。
    #[test]
    fn value_to_f64_only_accepts_numbers() {
        assert_eq!(value_to_f64(&Value::Float(-0.5)), Some(-0.5));
        // 整值（`Int` / 整值 `Float`）也走同一入口。
        assert_eq!(value_to_f64(&Value::Float(3.0)), Some(3.0));
        assert_eq!(value_to_f64(&Value::Str("1".into())), None);
        assert_eq!(value_to_f64(&Value::Bool(true)), None);
        assert_eq!(value_to_f64(&Value::Array(Vec::new())), None);
        assert_eq!(value_to_f64(&Value::Object(Default::default())), None);
    }

    /// 决策（2026-09-18）：**JSON 数字来源不猜整型**。`serde_json` 的整/浮形态
    /// 取决于文本写法（`1` vs `1.0`），不是可靠信号 —— `json_to_value` 必须永远
    /// 落 [`Value::Float`]，只有箭头列类型（Int64 / Timestamp(Ns)）才产出
    /// [`Value::Int`]。本用例防止后续“顺手”把整值 JSON 猜成 `Int`。
    #[test]
    fn json_numbers_stay_float_never_guessed_as_int() {
        for text in ["1", "1.0", "0", "-0", "9007199254740993", "1e18", "1.5"] {
            let json: serde_json::Value = serde_json::from_str(text).unwrap();
            let got = json_to_value(json).expect("json number → Some");
            assert!(
                matches!(got, Value::Float(_)),
                "JSON `{text}` 必须落 Float，实际 {got:?}"
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
            items.iter().all(|v| matches!(v, Value::Float(_))),
            "嵌套元素也不猜: {items:?}"
        );
    }
}
