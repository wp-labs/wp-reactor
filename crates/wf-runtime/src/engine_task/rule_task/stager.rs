//! 中间管道列式装载（rule 侧 pipe 通道化）：`PipeBatchStager` 列缓冲 + 惰性
//! 形状解析（`resolve_pipe_shape`）+ `PipeRowSink` 流式装载适配。
//! 主模块 `RuleTask` 的 emit 侧（emit_each_pipe_batch_columnar /
//! stage_pipe_record / flush_pipes）使用。

use std::sync::Arc;

use arrow::array::new_null_array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceRawErr, ToStructError};
use wf_engine::alert::OutputRecord;
use wf_engine::match_engine::{Event, PipeRowSink, batch_to_events};
use wf_engine::normalize_epoch_timestamp_float_nanos;
use wf_engine::window::Router;

use crate::error::{RuntimeReason, RuntimeResult};

#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use wf_lang::wfu_meta::{WFU_INTERMEDIATE_META_FIELDS, WfuIntermediateMetaField};

/// 中间管道事件时间字段（`__wf_pipe_ts` 特殊列：直写事件时间，绕过 schema
/// 时间列）。
pub(crate) const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";

/// Staged pipe batch: (window name, events) or `None` when nothing staged.
type PendingEventBatch = Option<(Arc<str>, Arc<Vec<Arc<Event>>>, RecordBatch)>;

/// Columnar staging state for the intermediate-target emit path
/// (rule-side channelization).
pub(crate) enum PipeState {
    /// No intermediate row emitted yet; the pipe shape resolves lazily on
    /// first use (the pipe registry may still be populating at boot).
    Uninit,
    /// Shape resolved; rows accumulate in the column buffers until the next
    /// `RuleTask::flush_pipes`.
    Staging(PipeBatchStager),
    /// Target window/pipe missing (warned once); rows are dropped — the
    /// same terminal behavior as the old per-row fallback.
    Dead,
}

/// Per-column staging buffer. The variant is chosen once from the pipe
/// schema; every row appends exactly one value (or null).
/// 中间管道列装载缓冲。
///
/// 2026-08-25（pipe 写入分配足迹）：从 `Vec<Option<T>>` 改为 **Arrow builder**。
/// 原实现的两处浪费（`malloc_history` + `pipe_write_alloc_footprint` 实证）：
/// 1. `Vec<Option<i64>>` 是 **16B/值**（Option 对齐填充），Arrow 目标只需
///    8B + 1bit —— builder 内部就是 `Vec<i64>` + null bitmap，直接省一半。
/// 2. `Vec<Option<String>>` 每行一次 **String 堆分配**（3 个字符串列 × 35,360
///    行 ≈ 10.6 万次/批），`StringBuilder` 追加进共享值缓冲 + offsets，
///    per-row 分配归零。
/// 3. `take_batch` 原先 `Int64Array::from(Vec<Option<_>>)` 是**全量拷贝**，
///    改 `builder.finish()` 后是缓冲移动（零拷贝）。
enum PipeCol {
    Int64(arrow::array::Int64Builder),
    Float64(arrow::array::Float64Builder),
    Bool(arrow::array::BooleanBuilder),
    Utf8(arrow::array::StringBuilder),
    Timestamp(arrow::array::TimestampNanosecondBuilder),
    /// Column types outside the supported coercion matrix stage as null —
    /// same fallback arm as `value_to_single_row_array`.
    Null {
        data_type: DataType,
        len: usize,
    },
}

/// 列式装载（q13a 列式化，2026-08-25）的列来源计划：schema 每列的值来自
/// yield 值（按 yield 下标）/ `__wf_pipe_ts` / `_wfu_meta_*` 回退 / 无来源。
/// 构造一次（new_columnar），`push_row` 逐行直查——免行式 `push_record` 的
/// 每列 × 每字段名字符串查找。
#[derive(Clone)]
enum PipeColSource {
    Yield(usize),
    EventTime,
    MetaRuleName,
    MetaScore,
    MetaEntityType,
    MetaEntityId,
    Missing,
}

/// Resolved shape of an intermediate pipe target: the relay schema and its
/// time column (pipe registry first, window fallback).
pub(crate) fn resolve_pipe_shape(
    pipe_registry: &Arc<wf_engine::pipe::PipeRegistry>,
    router: &Arc<Router>,
    target: &Arc<str>,
) -> Option<(arrow::datatypes::SchemaRef, Option<usize>)> {
    match pipe_registry.get(target) {
        // Pipe registered with a real schema (normal boot) → use it.
        Some(pipe) if !pipe.schema.fields().is_empty() => Some((pipe.schema, pipe.time_col_index)),
        // Pipe absent or built without schemas (e.g. the reload path builds
        // the registry with no window schemas) → fall back to the window,
        // which is always populated with the correct schema + time column.
        _ => router
            .registry()
            .get_window(target)
            .map(|win| (win.schema().clone(), win.time_col_index())),
    }
}

impl PipeBatchStager {
    /// 按 schema 建列 builder。`capacity` = 行数预估（R4 review，2026-08-25）：
    /// builder 用 `::new()` 时每批从 0 **倍增长**，`finish()` 后 buffer 容量可达
    /// len 的 2×——这部分**容量宽余是真实占用**（存活批次实测 6.03MB vs
    /// content 3.45MB），但 `window.allocated_bytes` 只计 buffer len 看不见。
    /// 按上一批行数预置容量即可避免倍增（中间窗批大小稳定 ≈ 35k 行）。
    fn make_cols(schema: &arrow::datatypes::SchemaRef, capacity: usize) -> Vec<PipeCol> {
        schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int64 => {
                    PipeCol::Int64(arrow::array::Int64Builder::with_capacity(capacity))
                }
                DataType::Float64 => {
                    PipeCol::Float64(arrow::array::Float64Builder::with_capacity(capacity))
                }
                DataType::Boolean => {
                    PipeCol::Bool(arrow::array::BooleanBuilder::with_capacity(capacity))
                }
                // StringBuilder 需两个容量：offsets 条数与值字节数（估 16B/值，
                // 中间窗 meta 列典型值长 5~12B）。
                DataType::Utf8 => PipeCol::Utf8(arrow::array::StringBuilder::with_capacity(
                    capacity,
                    capacity * 16,
                )),
                DataType::Timestamp(_, _) => PipeCol::Timestamp(
                    arrow::array::TimestampNanosecondBuilder::with_capacity(capacity),
                ),
                other => PipeCol::Null {
                    data_type: other.clone(),
                    len: 0,
                },
            })
            .collect()
    }

    pub(crate) fn new(
        target: Arc<str>,
        schema: arrow::datatypes::SchemaRef,
        time_col_index: Option<usize>,
    ) -> Self {
        let cols = Self::make_cols(&schema, 0);
        Self {
            target,
            schema,
            time_col_index,
            cols,
            rows: 0,
            col_sources: Vec::new(),
            yield_names: Vec::new(),
        }
    }

    /// 列式装载构造（q13a 列式化，2026-08-25）：额外预计算 schema 列 →
    /// yield/meta 来源映射（`yield_names` = yield 计划字段名顺序），`push_row`
    /// 逐行 O(cols) 直查，免 `push_record` 的每列名字符串查找。
    pub(crate) fn new_columnar(
        target: Arc<str>,
        schema: arrow::datatypes::SchemaRef,
        time_col_index: Option<usize>,
        yield_names: &[std::sync::Arc<str>],
    ) -> Self {
        use wf_lang::wfu_meta::WfuIntermediateMetaField;
        let mut stager = Self::new(target, schema, time_col_index);
        let meta_names = [
            (
                WfuIntermediateMetaField::RuleName,
                PipeColSource::MetaRuleName,
            ),
            (WfuIntermediateMetaField::Score, PipeColSource::MetaScore),
            (
                WfuIntermediateMetaField::EntityType,
                PipeColSource::MetaEntityType,
            ),
            (
                WfuIntermediateMetaField::EntityId,
                PipeColSource::MetaEntityId,
            ),
        ];
        stager.col_sources = stager
            .schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == PIPE_EVENT_TIME_FIELD {
                    return PipeColSource::EventTime;
                }
                if let Some(yield_idx) = yield_names.iter().position(|n| **n == *field.name()) {
                    return PipeColSource::Yield(yield_idx);
                }
                if let Some((_, src)) = meta_names.iter().find(|(m, _)| m.name() == field.name()) {
                    return src.clone();
                }
                PipeColSource::Missing
            })
            .collect();
        stager.yield_names = yield_names.to_vec();
        stager
    }

    /// Stage one emitted row. The coercion matrix mirrors
    /// `value_to_single_row_array` exactly (including the event-time
    /// fallbacks for the pipe event-time field and the schema's time
    /// column), so a flushed batch is byte-identical to concatenating the
    /// per-row batches the old path produced.
    ///
    /// 2026-08-26 q4a：生产路径已切到 [`Self::push_record_columnar`]（列式），
    /// 本行式实现仅服务对拍测试（字节一致锁）。
    #[cfg(test)]
    pub(crate) fn push_record(&mut self, record: &OutputRecord) -> RuntimeResult<()> {
        let event_time_nanos = record.event_time_nanos;
        let fields = record_window_fields(record);
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let value = fields
                .iter()
                .find(|(name, _)| **name == *field.name())
                .map(|(_, value)| value);
            let is_event_time = field.name() == PIPE_EVENT_TIME_FIELD;
            let is_time_column = self.time_col_index == Some(idx);
            push_pipe_col(
                &mut self.cols[idx],
                value,
                is_event_time,
                is_time_column,
                event_time_nanos,
            )?;
        }
        self.rows += 1;
        Ok(())
    }

    /// 列式装载一条 `OutputRecord`（2026-08-26 q4a deferred 中间窗）：复用
    /// `new_columnar` 预计算的列来源计划，免 `push_record` 的
    /// `record_window_fields`（yield_fields clone + HashSet + meta 名 Arc::from
    /// 每行分配）。语义与 `push_record` 完全一致（meta/yield/EventTime 值来源
    /// 逐分支对齐），对拍测试钉死字节一致。
    ///
    /// 注：`yield_fields` 与 yield 计划**顺序对齐但可能缺项**（Optional 字段
    /// None 被过滤）→ 按名回退查找（yield 列数少，O(cols)）。
    pub(crate) fn push_record_columnar(&mut self, record: &OutputRecord) -> RuntimeResult<()> {
        use wf_engine::match_engine::Value;
        debug_assert_eq!(
            self.col_sources.len(),
            self.schema.fields().len(),
            "columnar stager 必须先 new_columnar 预计算来源计划"
        );
        // meta 值预构：SmolStr 内联（≤22B 无堆分配）——rule_name/entity_type/
        // entity_id 典型值均内联，比 Arc::from 更省。
        let meta_rule = Value::Str(record.rule_name.as_ref().into());
        let meta_entity_type = Value::Str(record.entity_type.as_ref().into());
        let meta_entity_id = Value::Str(record.entity_id.as_str().into());
        let event_time_nanos = record.event_time_nanos;
        for (idx, source) in self.col_sources.iter().enumerate() {
            let value: Option<&Value> = match source {
                PipeColSource::Yield(yield_idx) => {
                    self.yield_names.get(*yield_idx).and_then(|name| {
                        record
                            .yield_fields
                            .iter()
                            .find(|(n, _)| **n == **name)
                            .map(|(_, v)| v)
                    })
                }
                PipeColSource::EventTime => None,
                PipeColSource::MetaRuleName => Some(&meta_rule),
                PipeColSource::MetaScore => Some(&Value::Number(record.score)),
                PipeColSource::MetaEntityType => Some(&meta_entity_type),
                PipeColSource::MetaEntityId => Some(&meta_entity_id),
                PipeColSource::Missing => None,
            };
            let is_event_time = matches!(source, PipeColSource::EventTime);
            let is_time_column = self.time_col_index == Some(idx);
            push_pipe_col(
                &mut self.cols[idx],
                value,
                is_event_time,
                is_time_column,
                event_time_nanos,
            )?;
        }
        self.rows += 1;
        Ok(())
    }

    /// 列式装载一行（q13a 列式化，2026-08-25）：值来自预计算的列来源计划
    /// （yield 值 / `__wf_pipe_ts` / `_wfu_meta_*`），coercion 矩阵与
    /// [`Self::push_record`] 逐分支一致（对拍测试钉死）。`rule_name` 供
    /// `_wfu_meta_rule_name` 回退列。
    ///
    /// 2026-08-25（pipe 写入分配足迹）：参数改为**借用切片**，供 executor 的
    /// 流式 sink 直接调用——不再需要先物化 `PipeEachRow`（每行一个 values Vec
    /// + 一个 entity_id String，实测 404 B/行）。
    pub(crate) fn push_row_parts(
        &mut self,
        rule_name: &str,
        score: f64,
        entity_type: &str,
        entity_id: &str,
        values: &[Option<wf_engine::match_engine::Value>],
        event_time_nanos: i64,
    ) -> RuntimeResult<()> {
        use wf_engine::match_engine::Value;
        debug_assert_eq!(
            self.col_sources.len(),
            self.schema.fields().len(),
            "columnar stager 必须先 new_columnar 预计算来源计划"
        );
        for (idx, source) in self.col_sources.iter().enumerate() {
            let value = match source {
                PipeColSource::Yield(yield_idx) => values.get(*yield_idx).and_then(|v| v.as_ref()),
                PipeColSource::EventTime => None,
                PipeColSource::MetaRuleName => Some(&Value::Str(rule_name.into())),
                PipeColSource::MetaScore => Some(&Value::Number(score)),
                PipeColSource::MetaEntityType => Some(&Value::Str(entity_type.into())),
                PipeColSource::MetaEntityId => Some(&Value::Str(entity_id.into())),
                PipeColSource::Missing => None,
            };
            let is_event_time = matches!(source, PipeColSource::EventTime);
            let is_time_column = self.time_col_index == Some(idx);
            push_pipe_col(
                &mut self.cols[idx],
                value,
                is_event_time,
                is_time_column,
                event_time_nanos,
            )?;
        }
        self.rows += 1;
        Ok(())
    }

    /// `PipeEachRow` 形态的装载（bench 对照 / 对拍测试用）——委托
    /// [`Self::push_row_parts`]，语义完全一致。
    #[cfg(test)]
    pub(crate) fn push_row(
        &mut self,
        rule_name: &str,
        row: &wf_engine::match_engine::PipeEachRow,
        event_time_nanos: i64,
    ) -> RuntimeResult<()> {
        self.push_row_parts(
            rule_name,
            row.score,
            &row.entity_type,
            &row.entity_id,
            &row.values,
            event_time_nanos,
        )
    }

    /// Build the staged rows into one batch and parse it to events,
    /// resetting the buffers. Returns `None` when nothing is staged.
    pub(crate) fn take_events(&mut self) -> RuntimeResult<PendingEventBatch> {
        let Some((target, batch)) = self.take_batch()? else {
            return Ok(None);
        };
        let events: Arc<Vec<Arc<Event>>> =
            Arc::new(batch_to_events(&batch).into_iter().map(Arc::new).collect());
        Ok(Some((target, events, batch)))
    }

    /// Build the staged rows into one batch, resetting the buffers — **without**
    /// materializing per-row `Event`s. 2026-08-25 q13 分片内存：events 物化
    /// （36.5k Event HashMap ≈ 18MB/批）只服务广播的 row-path 下游；列式
    /// 消费者（q13b 列式 join 从 raw batch 读）与窗口读者（q4b stats）都不
    /// 需要。广播裁剪 batch-only 后，分片积压（169 批 × 18MB ≈ 24GB 在途）
    /// 消除——q13a 分片放开后 RSS 28.8GB 的平台期主因。
    pub(crate) fn take_batch(&mut self) -> RuntimeResult<Option<(Arc<str>, RecordBatch)>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let arrays: Vec<arrow::array::ArrayRef> = self
            .cols
            .iter_mut()
            .map(|col| match col {
                // `finish()` 移动 builder 内部缓冲（零拷贝）并重置 builder 供下一批
                // 复用——原实现的 `XxxArray::from(std::mem::take(vec))` 是全量拷贝。
                PipeCol::Int64(b) => Ok(std::sync::Arc::new(b.finish()) as arrow::array::ArrayRef),
                PipeCol::Float64(b) => {
                    Ok(std::sync::Arc::new(b.finish()) as arrow::array::ArrayRef)
                }
                PipeCol::Bool(b) => Ok(std::sync::Arc::new(b.finish()) as arrow::array::ArrayRef),
                PipeCol::Utf8(b) => Ok(std::sync::Arc::new(b.finish()) as arrow::array::ArrayRef),
                PipeCol::Timestamp(b) => {
                    Ok(std::sync::Arc::new(b.finish()) as arrow::array::ArrayRef)
                }
                PipeCol::Null { data_type, len } => {
                    let array = new_null_array(data_type, *len);
                    *len = 0;
                    Ok(array)
                }
            })
            .collect::<RuntimeResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new(std::sync::Arc::clone(&self.schema), arrays)
            .source_raw_err(RuntimeReason::Bootstrap, "build internal pipeline batch")?;
        // R4 review 试验与**回退记录**（2026-08-25）：曾改为按刚完成行数预置下一批
        // builder 容量（`make_cols(&schema, finished_rows)`），想消除 `::new()` 倍增长
        // 留下的容量宽余。**实测否决**（`pipe_write_alloc_footprint`）：峰值
        // 6.88MB → 11.53MB（+67%）——预置会在刚完成的批次**仍存活**时就把下一批
        // 全量 builder（~4.6MB）分配出来，而保留批次的宽余没有可测改善。
        // 因此保持容量 0（倍增长），宽余作为已知成本记在 issue 文档。
        self.cols = Self::make_cols(&self.schema, 0);
        self.rows = 0;
        Ok(Some((Arc::clone(&self.target), batch)))
    }
}

pub(crate) struct PipeBatchStager {
    pub(crate) target: Arc<str>,
    schema: arrow::datatypes::SchemaRef,
    time_col_index: Option<usize>,
    cols: Vec<PipeCol>,
    rows: usize,
    /// 列式装载（`new_columnar`）的列来源计划；行式 `push_record` 不填
    /// （按名查找），`push_row` 要求非空。
    col_sources: Vec<PipeColSource>,
    /// yield 计划字段名（`new_columnar` 保存，供 `push_record_columnar`
    /// 按名回退——yield_fields 与计划顺序对齐但 Optional 缺失被过滤）。
    yield_names: Vec<Arc<str>>,
}

/// 共享的单列装载（q13a 列式化，2026-08-25）：`push_record` 与 `push_row`
/// 的 coercion 矩阵逐分支一致——Int64/Float64/Bool 数值转换、Utf8 的
/// Str/Number/Bool/JSON 渲染、Timestamp 的 epoch-ns 归一化与时间列回退、
/// Null 占位。`is_event_time_field` = `__wf_pipe_ts` 特殊列（直写
/// event_time_nanos）；`is_time_column` = schema 时间列（value 缺失时回退
/// event_time_nanos，与行式 Timestamp None 分支一致）。
fn push_pipe_col(
    col: &mut PipeCol,
    value: Option<&wf_engine::match_engine::Value>,
    is_event_time_field: bool,
    is_time_column: bool,
    event_time_nanos: i64,
) -> RuntimeResult<()> {
    if is_event_time_field && !matches!(col, PipeCol::Timestamp(_) | PipeCol::Null { .. }) {
        unreachable!("event-time field must be Timestamp");
    }
    match col {
        PipeCol::Int64(b) => b.append_option(match value {
            Some(wf_engine::match_engine::Value::Number(n)) => Some(*n as i64),
            _ => None,
        }),
        PipeCol::Float64(b) => b.append_option(match value {
            Some(wf_engine::match_engine::Value::Number(n)) => Some(*n),
            _ => None,
        }),
        PipeCol::Bool(b) => b.append_option(match value {
            Some(wf_engine::match_engine::Value::Bool(v)) => Some(*v),
            _ => None,
        }),
        PipeCol::Utf8(b) => {
            // 字符串列是分配大头（3 列 × 35,360 行 ≈ 10.6 万次 String/批）：
            // `Value::Str` 直接 `append_value(&str)`（**零中间 String**）；
            // Number/Bool 用 `write!` 写进 builder 的共享值缓冲（同样零分配，
            // 依赖 StringBuilder 实现了 `std::fmt::Write`）。数值渲染格式与
            // 原 `n.to_string()` / `b.to_string()` 逐字节一致（Display 实现相同）。
            match value {
                Some(wf_engine::match_engine::Value::Str(s)) => b.append_value(s.as_str()),
                Some(wf_engine::match_engine::Value::Number(n)) => {
                    use std::fmt::Write as _;
                    write!(b, "{n}").ok();
                    b.append_value("");
                }
                Some(wf_engine::match_engine::Value::Bool(v)) => {
                    use std::fmt::Write as _;
                    write!(b, "{v}").ok();
                    b.append_value("");
                }
                Some(
                    value @ (wf_engine::match_engine::Value::Array(_)
                    | wf_engine::match_engine::Value::Object(_)),
                ) => b.append_value(value_to_json_string(value)?),
                _ => b.append_null(),
            }
        }
        PipeCol::Timestamp(b) => {
            if is_event_time_field {
                b.append_value(event_time_nanos);
            } else {
                b.append_option(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => {
                        normalize_epoch_timestamp_float_nanos(*n)
                    }
                    // The schema's time column falls back to the row's event
                    // time when the yield did not provide one.
                    None if is_time_column => Some(event_time_nanos),
                    _ => None,
                });
            }
        }
        PipeCol::Null { len, .. } => *len += 1,
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn record_window_fields(
    record: &OutputRecord,
) -> Vec<(std::sync::Arc<str>, wf_engine::match_engine::Value)> {
    let mut fields = record.yield_fields.clone();
    let existing: HashSet<&str> = fields.iter().map(|(name, _)| &**name).collect();
    let missing_meta: Vec<WfuIntermediateMetaField> = WFU_INTERMEDIATE_META_FIELDS
        .iter()
        .copied()
        .filter(|field| !existing.contains(field.name()))
        .collect();
    for field in missing_meta {
        fields.push((
            std::sync::Arc::from(field.name()),
            record_wfu_intermediate_meta_value(record, field),
        ));
    }
    fields
}

#[cfg(test)]
pub(crate) fn record_wfu_intermediate_meta_value(
    record: &OutputRecord,
    field: WfuIntermediateMetaField,
) -> wf_engine::match_engine::Value {
    use wf_engine::match_engine::Value;

    match field {
        WfuIntermediateMetaField::RuleName => Value::Str(record.rule_name.clone().into()),
        WfuIntermediateMetaField::Score => Value::Number(record.score),
        WfuIntermediateMetaField::EntityType => Value::Str(record.entity_type.clone().into()),
        WfuIntermediateMetaField::EntityId => Value::Str(record.entity_id.clone().into()),
    }
}

pub(crate) fn value_to_json_string(
    value: &wf_engine::match_engine::Value,
) -> RuntimeResult<String> {
    serde_json::to_string(&value_to_json(value)?).source_raw_err(
        RuntimeReason::Bootstrap,
        "serialize structured pipeline value",
    )
}

pub(crate) fn value_to_json(
    value: &wf_engine::match_engine::Value,
) -> RuntimeResult<serde_json::Value> {
    match value {
        wf_engine::match_engine::Value::Number(n) if n.is_finite() => {
            Ok(serde_json::Value::from(*n))
        }
        wf_engine::match_engine::Value::Number(_) => RuntimeReason::Bootstrap
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
        wf_engine::match_engine::Value::Str(s) => Ok(serde_json::Value::from(s.as_str())),
        wf_engine::match_engine::Value::Bool(b) => Ok(serde_json::Value::from(*b)),
        wf_engine::match_engine::Value::Array(items) => Ok(serde_json::Value::Array(
            items
                .iter()
                .map(value_to_json)
                .collect::<RuntimeResult<Vec<_>>>()?,
        )),
        wf_engine::match_engine::Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

/// `PipeBatchStager` 的 [`PipeRowSink`] 适配器（2026-08-25 pipe 写入流式装载）：
/// executor 逐行回调，直接装列——省掉整批 `Vec<PipeEachRow>` 中间层。
/// `rule_name` 供 `__wfu_rule_name` 回退列；`errors` 聚合本批装载失败数
/// （每行一条 warn 会在 3.5 万行批上淹没日志，故聚合后一次性上报）。
pub(crate) struct PipeStagerSink<'a> {
    pub(crate) stager: &'a mut PipeBatchStager,
    pub(crate) rule_name: &'a str,
    pub(crate) errors: usize,
}

impl PipeRowSink for PipeStagerSink<'_> {
    fn push_pipe_row(
        &mut self,
        score: f64,
        entity_type: &str,
        entity_id: &str,
        values: &[Option<wf_engine::match_engine::Value>],
        event_time_nanos: i64,
    ) -> Result<(), String> {
        match self.stager.push_row_parts(
            self.rule_name,
            score,
            entity_type,
            entity_id,
            values,
            event_time_nanos,
        ) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.errors += 1;
                Err(e.to_string())
            }
        }
    }
}
