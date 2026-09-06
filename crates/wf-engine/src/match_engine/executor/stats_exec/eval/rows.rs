//! rows — eval/ 子模块（从 eval.rs 拆分）。
use super::*;

pub(crate) fn field_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(n) => n,
        FieldRef::Qualified(_, n) | FieldRef::Bracketed(_, n) => n,
        FieldRef::Path { segments, .. } => match segments.first() {
            Some(wf_lang::ast::PathSegment::Field(root)) => root,
            _ => "",
        },
        _ => "",
    }
}

/// 度量字段在行字段列数组中的位置: 子集模式走构造期预计算; 无子集按
/// `names` 名查（last/top 且字段在列内 → Some, 其余 None）。
/// 自由函数而非方法: 调用点同时持有 `&mut self.window` 的桶借用, 方法会整
/// self 借用冲突; 自由函数只借 `plan` + `measure_field_idx` 两字段。
pub(crate) fn measure_field_position(
    plan: &StatsPlan,
    measure_field_idx: &[Option<usize>],
    idx: usize,
    names: Option<&[String]>,
) -> Option<usize> {
    match measure_field_idx[idx] {
        Some(i) => Some(i),
        None => match (&plan.measures[idx].field, names) {
            (Some(f), Some(ns)) => ns.iter().position(|n| n == field_name(f)),
            _ => None,
        },
    }
}

/// last/top 行更新（Q18/Q19, 非归并状态）:
/// - `last`: 最近合格行的行字段列数组替换（流有序 = 事件时间最新, 权威 Q18
///   ORDER BY dateTime DESC）; 同桶多 last 度量共享同一 Arc（内存 1 份）。
/// - `top`: 按 key DESC 插入有界 top-N（同 key 先到者优先——流有序下的确定性
///   tie-break, 权威 Q19 未指定平局顺序）。
///
/// `field_idx` = 度量字段在行字段列数组中的位置（P5 预计算, 免字符串查找）;
/// `None` = 字段不在子集/无字段 → top 无键跳过, last 仍保留行。
pub(crate) fn apply_last_top(
    acc: &mut StatsAccum,
    measure: &StatsMeasurePlan,
    row: &std::sync::Arc<RowFields>,
    field_idx: Option<usize>,
) {
    match measure.agg {
        StatsAggPlan::Last => {
            *acc.last_mut() = Some(std::sync::Arc::clone(row));
        }
        StatsAggPlan::Top => {
            let Some(key) = field_idx.and_then(|i| row.f64_at(i)) else {
                return; // 非数值键 → 跳过（与 sum 跳过非数值一致）
            };
            let n = measure.arg.unwrap_or(10) as usize;
            if n == 0 {
                return; // top(0, ...): 不保留任何条目
            }
            let entries = acc.top_mut();
            // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目
            // 必插在既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义
            // 不变, 免去每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
            if entries.len() == n && key <= entries[n - 1].key {
                return;
            }
            // Arc 深拷贝为独立 Box（top 条目各自的行, 不共享）
            insert_top(entries, key, row.as_ref().clone(), n);
        }
        _ => {}
    }
}

/// top-N 插入: key DESC 有序保留前 N; 同 key 新条目插在已有同 key 条目之后
/// （先到者在前）。n=0 时清空（top(0, ...) 边界）。
pub(crate) fn insert_top(entries: &mut Vec<TopEntry>, key: f64, row: RowFields, n: usize) {
    if n == 0 {
        return;
    }
    // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目必插在
    // 既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义不变, 免去
    // 每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
    if entries.len() == n && key <= entries[n - 1].key {
        return;
    }
    let pos = entries
        .iter()
        .position(|e| key > e.key)
        .unwrap_or(entries.len());
    entries.insert(pos, TopEntry { key, row });
    if entries.len() > n {
        entries.truncate(n);
    }
}

pub(crate) fn value_to_distinct_key(v: &Value) -> DistinctKey {
    match v {
        Value::Number(n) => DistinctKey::from_f64(*n),
        Value::Str(s) => DistinctKey::from_str(s),
        Value::Bool(b) => DistinctKey::Int(if *b { 1 } else { 0 }),
        _ => DistinctKey::Str(format!("{:?}", v).into()),
    }
}

/// 行式 last/top 行字段提取（与列式 [`row_fields_from_batch`] 对齐, P5 紧凑化）:
/// 按 `names` 列序返回 `Box<[Option<Value>]>`（缺失 = `None`）。`None` = 全列,
/// 行键**排序**（确定性; 仅测试/缺省——生产经 spawn 恒有子集）。
pub(crate) fn row_fields_from_row(
    row: &HashMap<String, Value>,
    names: Option<&[String]>,
    layout: &std::sync::Arc<RowFieldLayout>,
) -> std::sync::Arc<RowFields> {
    let mut fields = RowFields::empty(std::sync::Arc::clone(layout));
    match names {
        Some(ns) => {
            for (i, n) in ns.iter().enumerate() {
                fields.set(i, row.get(n).cloned());
            }
        }
        None => {
            let mut keys: Vec<&String> = row.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                fields.set(i, row.get(*k).cloned());
            }
        }
    }
    std::sync::Arc::new(fields)
}

/// 从 batch 行提取字段列数组（last/top 列式路径用, P5 紧凑化）: 按 `cols`
/// （每字段列索引, 每批预解析一次——免逐行 `schema.index_of`）提取, null/缺失 =
/// `None`。`cols = None` 时全部 schema 列按字段名**排序**（与行式 None 同序——
/// 行键 == schema 字段时两路径列序一致; 测试/缺省路径）。
pub(crate) fn row_fields_from_batch(
    batch: &RecordBatch,
    row: usize,
    cols: Option<&[Option<usize>]>,
    layout: &std::sync::Arc<RowFieldLayout>,
) -> std::sync::Arc<RowFields> {
    let schema = batch.schema();
    let mut fields = RowFields::empty(std::sync::Arc::clone(layout));
    match cols {
        Some(cols) => {
            for (i, ci) in cols.iter().enumerate() {
                // 字段缺失 → None
                let v = ci.and_then(|ci| batch_cell_value(batch, &schema, ci, row));
                fields.set(i, v);
            }
        }
        None => {
            let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            names.sort();
            for (i, name) in names.iter().enumerate() {
                let col_idx = schema.index_of(name).expect("schema 字段必存在");
                fields.set(i, batch_cell_value(batch, &schema, col_idx, row));
            }
        }
    }
    std::sync::Arc::new(fields)
}

/// 从 batch 读单格字段值：null → `None`，否则按 schema 字段类型抽取。
fn batch_cell_value(
    batch: &RecordBatch,
    schema: &arrow::datatypes::SchemaRef,
    ci: usize,
    row: usize,
) -> Option<Value> {
    let col = batch.column(ci);
    if col.is_null(row) {
        None
    } else {
        extract_field_value(schema.field(ci), col.as_ref(), row)
    }
}

/// 从 batch 列读单行原生数值（Int64 原生 i64 → i128, 不走 f64——D8: ≥2^53 的
/// Int64 经 `Value::Number(f64)` 会丢精度; Float64 按 `sum_masked` 同口径截断）。
/// null / 非数值列 → None（与行式 `value_to_i128` 的 None 一致）。
pub(crate) fn column_i128_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<i128> {
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as i128);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row) as i128);
    }
    None
}

/// 从 batch 列读单行原生数值（top 快速淘汰预检用; 列索引预解析, 零 index_of）。
/// Int64 → as f64 / Float64 → 原值——与行字段提取后 `value_to_f64(Value::Number)`
/// 同口径（event_bridge 契约: Int64 → Number(i as f64), Float64 → Number(f)）。
/// 非数值类型 → None（调用方回退原路径, 语义不变）。
pub(crate) fn column_f64_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<f64> {
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    None
}

/// 索引版 distinct 键读取（列索引批级预解析, 免每行 schema.index_of——q17 同款修复）。
/// 与列式段 `insert_distinct_column` 同类型分派, 原生值构造（D7: 禁止
/// `Value::Number(f64)` 化 ≥2^53 的 Int64）。null / 类型不在支持集 → None。
pub(crate) fn column_distinct_key_at(
    batch: &RecordBatch,
    ci: usize,
    row: usize,
) -> Option<DistinctKey> {
    use arrow::array::TimestampNanosecondArray;
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(DistinctKey::from_i64(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(DistinctKey::from_f64(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Some(DistinctKey::from_str(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return Some(DistinctKey::from_f64(if a.value(row) { 1.0 } else { 0.0 }));
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(DistinctKey::from_i64(a.value(row)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_name_extracts_simple_qualified_and_path_roots() {
        assert_eq!(field_name(&FieldRef::Simple("k".into())), "k");
        assert_eq!(
            field_name(&FieldRef::Qualified("e".into(), "dip".into())),
            "dip"
        );
        assert_eq!(
            field_name(&FieldRef::Bracketed("e".into(), "ports".into())),
            "ports"
        );
        assert_eq!(
            field_name(&FieldRef::Path {
                alias: "e".into(),
                segments: vec![wf_lang::ast::PathSegment::Field("root".into())],
            }),
            "root"
        );
        // 非 Field 首段 / 未知形态 → ""
        assert_eq!(
            field_name(&FieldRef::Path {
                alias: "e".into(),
                segments: vec![wf_lang::ast::PathSegment::Index(0)],
            }),
            ""
        );
    }

    #[test]
    fn value_to_distinct_key_routes_by_kind() {
        assert!(matches!(
            value_to_distinct_key(&Value::Bool(true)),
            DistinctKey::Int(1)
        ));
        assert!(matches!(
            value_to_distinct_key(&Value::Bool(false)),
            DistinctKey::Int(0)
        ));
        assert!(matches!(
            value_to_distinct_key(&Value::Str("x".into())),
            DistinctKey::Str(_)
        ));
        assert!(matches!(
            value_to_distinct_key(&Value::Number(3.5)),
            DistinctKey::Float(_) | DistinctKey::Int(_)
        ));
    }
}
