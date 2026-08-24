//! 批级 join-then-key scope key 预解析（2026-08-23，q4/q6）。
//!
//! `match<category:...>` 形态的 join-then-key 规则，其窗口键来自 snapshot join
//! 右窗字段（如 `b.auction == auction_events.id` → 键取 `auction_events.category`）。
//! 引擎逐事件执行 `advance` 时，每个事件都要做一次 join 索引 lookup + `values_equal`
//! 精确复核 + 右窗键字段物化（q4/q6 实测占 advance 路径 ~88.8%）。
//!
//! NEXMark bid 的 auction 引用高度集中（50% 热点集中在最近 100 个 auction），一批
//! 事件按驱动 key 去重后唯一键通常只有几十个。本模块对**一批**事件按驱动 key 去重
//! 后每唯一 key 只做一次 lookup，逐行产出预解析 scope key，`advance` 时直接使用
//! （`CepStateMachine::advance_at_with_masks_key` 的 `key_override`），跳过每事件
//! 内部解析。
//!
//! 语义与逐事件内部解析（`resolve_key_join_scope_key`）字节一致：
//! - int（非 float）驱动 key：索引截断精确 → `values_equal` 复核恒真 → 桶首行；
//! - float 驱动 key：逐行复核（同 key 不同 float 值容差复核结果可能不同，`1.5` 会
//!   截断假匹配到 `id=1`，必须复核拒绝）。
//!
//! 返回 per-row_domain-index 的 `Option<Vec<Value>>`（scope key）；`None` = 该行
//! 任一环节 miss（左字段缺失 / join miss / key 缺失）→ advance 跳过。

use std::collections::HashMap;

use wf_lang::plan::JoinKeyPlan;

use crate::match_engine::event_bridge::ColumnarEvent;
use crate::match_engine::match_engine::{
    JoinKey, Value, WindowLookup, field_ref_name, values_equal,
};

/// 批级 join-then-key scope key 预解析（见模块文档）。
pub fn precompute_join_then_keys(
    batch: &arrow::record_batch::RecordBatch,
    row_domain: &[usize],
    kjp: &JoinKeyPlan,
    windows: &impl WindowLookup,
) -> Vec<Option<Vec<Value>>> {
    let left_name = field_ref_name(&kjp.left_field);
    let Some(left_idx) = batch.schema().index_of(left_name).ok() else {
        return vec![None; row_domain.len()]; // 左列缺 → 全 miss（同逐事件）
    };
    let left_is_float = matches!(
        batch.schema().field(left_idx).data_type(),
        arrow::datatypes::DataType::Float32 | arrow::datatypes::DataType::Float64
    );

    let mut per_row_val: Vec<Option<Value>> = Vec::with_capacity(row_domain.len());
    let mut key_rows: HashMap<JoinKey, Vec<usize>> = HashMap::new();
    for (i, &row) in row_domain.iter().enumerate() {
        let val = ColumnarEvent::new(batch, row).value_at(left_idx);
        match val.as_ref().and_then(JoinKey::from_value) {
            Some(k) => {
                key_rows.entry(k).or_default().push(i);
                per_row_val.push(val);
            }
            None => per_row_val.push(None),
        }
    }

    let mut out: Vec<Option<Vec<Value>>> = vec![None; row_domain.len()];
    for idxs in key_rows.values() {
        if left_is_float {
            // 逐行复核（同 key 不同 float 值容差复核结果可能不同）。
            for &i in idxs {
                let lv = per_row_val[i].as_ref().expect("key_rows rows have values");
                out[i] = windows
                    .join_lookup(&kjp.right_window, &kjp.right_key_field, lv)
                    .and_then(|rs| {
                        rs.iter()
                            .find(|r| {
                                r.field_value(&kjp.right_key_field)
                                    .is_some_and(|rv| values_equal(lv, &rv))
                            })
                            .and_then(|r| r.field_value(&kjp.right_field))
                    })
                    .map(|v| vec![v]);
            }
        } else {
            // int：索引截断精确，桶首行即 find 首行（复核恒真，与内部一致）。
            let first = per_row_val[*idxs.first().unwrap()]
                .as_ref()
                .expect("key_rows rows have values");
            let key_val: Option<Value> = windows
                .join_lookup(&kjp.right_window, &kjp.right_key_field, first)
                .and_then(|rs| {
                    rs.iter()
                        .find(|r| {
                            r.field_value(&kjp.right_key_field)
                                .is_some_and(|rv| values_equal(first, &rv))
                        })
                        .and_then(|r| r.field_value(&kjp.right_field))
                });
            for &i in idxs {
                out[i] = key_val.clone().map(|v| vec![v]);
            }
        }
    }
    out
}
