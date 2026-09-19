use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use wf_engine::match_engine::{
    WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT,
    wfl_structured_field_kind,
};
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::error::{RuntimeReason, RuntimeResult};
use orion_error::conversion::ToStructError;

pub(super) fn validate_batch_schema_for_stream(
    schemas: &[WindowSchema],
    stream_name: &str,
    batch_schema: &Schema,
) -> RuntimeResult<()> {
    let expected = resolve_stream_schema(schemas, stream_name)?;
    if !schemas_are_compatible_for_stream(expected.as_ref(), batch_schema) {
        return RuntimeReason::data_error()
            .to_err()
            .with_detail(format!(
                "arrow source schema mismatch for stream {:?}",
                stream_name
            ))
            .err();
    }
    Ok(())
}

pub(crate) fn schemas_are_compatible_for_stream(expected: &Schema, actual: &Schema) -> bool {
    if expected == actual {
        return true;
    }
    if expected.fields().len() != actual.fields().len() {
        return false;
    }
    expected
        .fields()
        .iter()
        .zip(actual.fields())
        .all(|(expected, actual)| fields_are_compatible_for_stream(expected, actual))
}

fn fields_are_compatible_for_stream(expected: &Field, actual: &Field) -> bool {
    if expected.name() != actual.name() {
        return false;
    }
    match wfl_structured_field_kind(expected) {
        Some(expected_kind) => structured_field_is_compatible(expected_kind, actual),
        _ => expected == actual,
    }
}

fn structured_field_is_compatible(expected_kind: &str, actual: &Field) -> bool {
    match actual.data_type() {
        DataType::Utf8 => match wfl_structured_field_kind(actual) {
            Some(actual_kind) => actual_kind == expected_kind,
            None => true,
        },
        DataType::Struct(_) => expected_kind == WFL_FIELD_TYPE_OBJECT,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            expected_kind == WFL_FIELD_TYPE_ARRAY
        }
        _ => false,
    }
}

pub(crate) fn resolve_stream_schema(
    schemas: &[WindowSchema],
    stream_name: &str,
) -> RuntimeResult<SchemaRef> {
    maybe_resolve_stream_schema(schemas, stream_name)?.ok_or_else(|| {
        RuntimeReason::data_error()
            .to_err()
            .with_detail(format!("no schema subscribed for stream {:?}", stream_name))
    })
}

pub(crate) fn maybe_resolve_stream_schema(
    schemas: &[WindowSchema],
    stream_name: &str,
) -> RuntimeResult<Option<SchemaRef>> {
    let mut schema: Option<SchemaRef> = None;
    for ws in schemas {
        if !ws.streams.iter().any(|s| s == stream_name) {
            continue;
        }
        let candidate = window_schema_to_arrow(ws)?;
        if let Some(existing) = &schema {
            if existing.as_ref() != candidate.as_ref() {
                return RuntimeReason::data_error()
                    .to_err()
                    .with_detail(format!(
                        "stream {:?} maps to inconsistent schemas (window {:?})",
                        stream_name, ws.name
                    ))
                    .err();
            }
        } else {
            schema = Some(candidate);
        }
    }
    Ok(schema)
}

pub(crate) fn window_schema_to_arrow(ws: &WindowSchema) -> RuntimeResult<SchemaRef> {
    let mut fields = Vec::with_capacity(ws.fields.len());
    for field in &ws.fields {
        fields.push(field_to_arrow(&field.name, &field.field_type));
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn field_to_arrow(name: &str, field_type: &FieldType) -> Field {
    let field = Field::new(name, field_type_to_arrow(field_type), true);
    match structured_field_metadata_value(field_type) {
        Some(value) => field.with_metadata(HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            value.to_string(),
        )])),
        None => field,
    }
}

pub(crate) fn field_type_to_arrow(ft: &FieldType) -> DataType {
    match ft {
        FieldType::Base(base) => base_type_to_arrow(base),
        FieldType::ArrayAny | FieldType::Array(_) | FieldType::Object => DataType::Utf8,
    }
}

fn structured_field_metadata_value(ft: &FieldType) -> Option<&'static str> {
    match ft {
        FieldType::Object => Some(WFL_FIELD_TYPE_OBJECT),
        FieldType::ArrayAny | FieldType::Array(_) => Some(WFL_FIELD_TYPE_ARRAY),
        FieldType::Base(_) => None,
    }
}

fn base_type_to_arrow(base: &BaseType) -> DataType {
    match base {
        BaseType::Chars | BaseType::Ip | BaseType::Hex => DataType::Utf8,
        BaseType::Digit => DataType::Int64,
        BaseType::Float => DataType::Float64,
        BaseType::Bool => DataType::Boolean,
        BaseType::Time => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wf_lang::FieldDef;

    fn field_def(name: &str, field_type: FieldType) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            field_type,
        }
    }

    fn window(name: &str, streams: &[&str], fields: Vec<FieldDef>) -> WindowSchema {
        WindowSchema {
            name: name.to_string(),
            streams: streams.iter().map(|s| s.to_string()).collect(),
            time_field: None,
            over: std::time::Duration::ZERO,
            fields,
        }
    }

    #[test]
    fn type_conversions_and_metadata() {
        assert_eq!(base_type_to_arrow(&BaseType::Digit), DataType::Int64);
        assert_eq!(
            base_type_to_arrow(&BaseType::Time),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(field_type_to_arrow(&FieldType::Object), DataType::Utf8);
        // 结构化字段带类型元数据
        let f = field_to_arrow("obj", &FieldType::Object);
        assert_eq!(
            f.metadata()
                .get(WFL_FIELD_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some(WFL_FIELD_TYPE_OBJECT)
        );
        let f = field_to_arrow("arr", &FieldType::ArrayAny);
        assert_eq!(
            f.metadata()
                .get(WFL_FIELD_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some(WFL_FIELD_TYPE_ARRAY)
        );
        // 基础字段无元数据
        assert!(
            field_to_arrow("n", &FieldType::Base(BaseType::Digit))
                .metadata()
                .is_empty()
        );
    }

    #[test]
    fn schema_compatibility_matrix() {
        let a = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        assert!(schemas_are_compatible_for_stream(&a, &a.clone()));
        // 列数不同 → 不兼容
        let b = Schema::new(Vec::<Field>::new());
        assert!(!schemas_are_compatible_for_stream(&a, &b));
        // 类型不同 → 不兼容
        let c = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
        assert!(!schemas_are_compatible_for_stream(&a, &c));
        // 结构化字段: object 期待 vs struct 列 → 兼容; vs list 列 → 不兼容
        let obj_meta = HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]);
        let exp_obj = Schema::new(vec![
            Field::new("v", DataType::Utf8, true).with_metadata(obj_meta),
        ]);
        let actual_struct = Schema::new(vec![Field::new(
            "v",
            DataType::Struct(arrow::datatypes::Fields::from([std::sync::Arc::new(
                Field::new("k", DataType::Utf8, true),
            )])),
            true,
        )]);
        assert!(schemas_are_compatible_for_stream(&exp_obj, &actual_struct));
        let actual_list = Schema::new(vec![Field::new(
            "v",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            true,
        )]);
        assert!(!schemas_are_compatible_for_stream(&exp_obj, &actual_list));
    }

    #[test]
    fn resolve_stream_schema_matches_windows() {
        let schemas = vec![
            window(
                "w1",
                &["s1", "s2"],
                vec![field_def("a", FieldType::Base(BaseType::Digit))],
            ),
            window(
                "w2",
                &["s3"],
                vec![field_def("b", FieldType::Base(BaseType::Float))],
            ),
        ];
        let r = resolve_stream_schema(&schemas, "s1").expect("resolve s1");
        assert_eq!(r.fields().len(), 1);
        assert_eq!(r.field(0).name(), "a");
        // 未订阅流 → None; resolve 报错
        assert!(
            maybe_resolve_stream_schema(&schemas, "nope")
                .unwrap()
                .is_none()
        );
        assert!(resolve_stream_schema(&schemas, "nope").is_err());
        // 两窗订阅同流但 schema 不一致 → 报错
        let dup = vec![
            window(
                "w1",
                &["s"],
                vec![field_def("a", FieldType::Base(BaseType::Digit))],
            ),
            window(
                "w2",
                &["s"],
                vec![field_def("a", FieldType::Base(BaseType::Float))],
            ),
        ];
        assert!(resolve_stream_schema(&dup, "s").is_err());
    }

    // -----------------------------------------------------------------
    // Arrow 类型契约钉桩 / 跨 crate 对拍
    // 规格表：wp-reactor/docs/design/arrow-type-mapping.md
    // -----------------------------------------------------------------

    /// 期望侧（线协议契约 = 规格表 A 列）全量钉桩。
    #[test]
    fn arrow_contract_expected_column_is_pinned() {
        let ts = DataType::Timestamp(TimeUnit::Nanosecond, None);
        // WPL 可声明的 7 种基础类型
        assert_eq!(base_type_to_arrow(&BaseType::Chars), DataType::Utf8);
        assert_eq!(base_type_to_arrow(&BaseType::Digit), DataType::Int64);
        assert_eq!(base_type_to_arrow(&BaseType::Float), DataType::Float64);
        assert_eq!(base_type_to_arrow(&BaseType::Bool), DataType::Boolean);
        assert_eq!(base_type_to_arrow(&BaseType::Time), ts);
        assert_eq!(base_type_to_arrow(&BaseType::Ip), DataType::Utf8);
        // 规格表 DIV-1：期望侧 Hex = Utf8（sink 侧当前给 Binary）
        assert_eq!(base_type_to_arrow(&BaseType::Hex), DataType::Utf8);

        // 结构化字段：一律 Utf8 + `wfl_field_type` 元数据
        for (name, ft, kind) in [
            ("o", FieldType::Object, WFL_FIELD_TYPE_OBJECT),
            ("a", FieldType::ArrayAny, WFL_FIELD_TYPE_ARRAY),
            ("a", FieldType::Array(BaseType::Digit), WFL_FIELD_TYPE_ARRAY),
        ] {
            assert_eq!(field_type_to_arrow(&ft), DataType::Utf8, "{name}");
            assert_eq!(
                field_to_arrow(name, &ft)
                    .metadata()
                    .get(WFL_FIELD_TYPE_METADATA_KEY)
                    .map(String::as_str),
                Some(kind),
                "{name}"
            );
        }
        // 基础字段不带结构化元数据
        assert!(
            field_to_arrow("n", &FieldType::Base(BaseType::Digit))
                .metadata()
                .is_empty()
        );
    }

    /// A ↔ C 真对拍：期望侧与 `wp-arrow` 对同一 WPL 类型必须给出同一个 Arrow 口径。
    ///
    /// `wf-runtime` 依赖 crates.io 的 `wp-arrow`（0.3.x，其 `schema.rs` 映射与
    /// `wfusion/wp-arrow` 本地版本逐字相同），所以这里调用的是**真实的另一份实现**，
    /// 而不是把期望值再抄一遍。对应规格表 §3 的 A/C 两列。
    ///
    /// 两侧各自 `Debug` 成字符串再比较，而不是直接 `assert_eq!`：`wp-arrow 0.3.1`
    /// 依赖 `arrow 59`，本 crate 升到 `arrow 60` 后 `DataType` 是另一个类型，
    /// 根本无法直接相等。重叠面全是标量类型，两种 arrow 的 Debug 文案一致。
    #[test]
    fn arrow_contract_matches_wp_arrow_on_overlap() {
        use wp_arrow::schema::{WpDataType, to_arrow_type};

        fn kind(dt: impl std::fmt::Debug) -> String {
            format!("{dt:?}")
        }

        let overlap = [
            (BaseType::Chars, WpDataType::Chars),
            (BaseType::Digit, WpDataType::Digit),
            (BaseType::Float, WpDataType::Float),
            (BaseType::Bool, WpDataType::Bool),
            (BaseType::Time, WpDataType::Time),
            (BaseType::Ip, WpDataType::Ip),
            (BaseType::Hex, WpDataType::Hex),
        ];
        for (wpl, wp) in overlap {
            assert_eq!(
                kind(base_type_to_arrow(&wpl)),
                kind(to_arrow_type(&wp)),
                "WPL {wpl:?} 与 wp-arrow {wp:?} 的 Arrow 口径分叉"
            );
        }

        // 规格表 DIV-3：结构化字段「形态」不同 —— 期望侧统一 Utf8(+kind 元数据)，
        // wp-arrow 给 List。接收侧靠 kind 元数据判定兼容，故当前可容忍。
        assert_eq!(
            kind(field_type_to_arrow(&FieldType::Array(BaseType::Digit))),
            "Utf8"
        );
        let wp_arrow_arr = kind(to_arrow_type(&WpDataType::Array(Box::new(
            WpDataType::Digit,
        ))));
        assert!(
            wp_arrow_arr.starts_with("List("),
            "DIV-3 形态差异，见规格表 §4：{wp_arrow_arr}"
        );
    }

    /// DIV-1 守卫：期望侧对 `hex` **只**接受 `Utf8`。
    ///
    /// 上游（`wp-connector-utils`）曾把这个字段映成 `Binary`（原始大端字节），
    /// 导致校验硬报错；现已对齐为 `Utf8`。本测试防止**任一侧**退回：
    /// 接收侧若放宽，会静默接受 `[0x1A, 0x2B]` 这类原始字节（与 `0x1A2B` 字符串
    /// 语义不同）；上游若回退，这里会再次硬报错。
    #[test]
    fn arrow_contract_hex_only_accepts_utf8() {
        let schemas = vec![window(
            "w",
            &["s"],
            vec![field_def("h", FieldType::Base(BaseType::Hex))],
        )];
        let raw_bytes = Schema::new(vec![Field::new("h", DataType::Binary, true)]);
        assert!(
            validate_batch_schema_for_stream(&schemas, "s", &raw_bytes).is_err(),
            "Binary 不被接受（DIV-1）；若此断言失败说明接收侧放宽了口径，请同步规格表 §4"
        );
    }

    /// DIV-1 对齐后的正例：`hex` 按 `Utf8`（期望侧 / wp-arrow / 修复后的 sink 口径）即通过。
    /// DIV-1 的修正方向：同一字段按 `Utf8`（期望侧 / wp-arrow 口径）即通过。
    #[test]
    fn arrow_contract_hex_utf8_is_accepted() {
        let schemas = vec![window(
            "w",
            &["s"],
            vec![field_def("h", FieldType::Base(BaseType::Hex))],
        )];
        let aligned = Schema::new(vec![Field::new("h", DataType::Utf8, true)]);
        assert!(validate_batch_schema_for_stream(&schemas, "s", &aligned).is_ok());
    }

    /// DIV-1（P0-2）**跨仓端到端**：`hex` 字段按 sink 侧实现（`wp-connector-utils`
    /// 的 `infer_schema_from_record`，经 `wp-core-connectors` 再导出）推断出的列类型，
    /// 必须能通过接收侧的窗口 schema 校验。
    ///
    /// 这正是 P0-2 的原始症状：`wp-connector-utils` 0.3.0 给 `Binary`（原始大端字节），
    /// 与期望侧的 `Utf8` 严格比较失败 → 「arrow source schema mismatch」。
    /// 0.3.1 起对齐为 `Utf8`，本用例把这条跨仓契约钉死。
    #[test]
    fn arrow_contract_sink_inferred_hex_schema_passes_the_receiver() {
        use wp_model_core::model::{DataRecord, Field as ModelField, FieldStorage, HexT};

        let rec = DataRecord::from(vec![FieldStorage::from(ModelField::from_hex(
            "h",
            HexT(0x1A2B),
        ))]);
        let inferred = wp_core_connectors::sinks::arrow_conv::infer_schema_from_record(&rec);
        assert_eq!(
            inferred.field(0).data_type(),
            &DataType::Utf8,
            "sink 侧（wp-connector-utils >= 0.3.1）必须把 hex 推断为 Utf8"
        );

        let schemas = vec![window(
            "w",
            &["s"],
            vec![field_def("h", FieldType::Base(BaseType::Hex))],
        )];
        assert!(
            validate_batch_schema_for_stream(&schemas, "s", &inferred).is_ok(),
            "跨仓契约：sink 推断出的 hex 列必须被接收侧接受（P0-2 回归）"
        );
    }

    /// 值层兜底口径：`Binary` 源列在 coerce 时不被支持，会整列变 `Null`。
    /// 这条钉桩说明「绕过 schema 校验也不能救回数据」（规格表 §2）。
    ///
    /// 注意断言的是 `Null` **类型**而不是 `null_count()`：arrow 的 `NullArray`
    /// 没有 validity buffer，`null_count()` 返回 0，容易被误读成「没丢值」。
    #[test]
    fn arrow_contract_binary_source_degrades_to_null() {
        use crate::receiver::route::coerce_column;
        use arrow::array::{Array as _, ArrayRef, BinaryArray, NullArray};

        let src: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0x1Au8, 0x2B][..]), None]));
        let coerced = coerce_column(&src, &DataType::Utf8, 2);
        assert_eq!(coerced.len(), 2);
        assert_eq!(coerced.data_type(), &DataType::Null);
        assert!(
            coerced.as_any().downcast_ref::<NullArray>().is_some(),
            "Binary → Utf8 无转换路径，整列退化为 NullArray"
        );
    }
}
