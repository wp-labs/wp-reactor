//! `AlertColumnBuilder` L3 批提交与收口面（2026-09-04 自 column_batch.rs 拆出；
//! `#[path]` 兄弟子模块）：`commit_each_rows_batch`（on-each 列主序批提交）、
//! `commit_close_rows_batch`（close 路径 per-row 变值批提交）与 `finish`（封印为
//! 不可变 `AlertColumnBatch`）。逐行装载/C2 staging 面见 `column_batch_stage.rs`；
//! 类型声明与收口在父 `column_batch.rs`。

use smol_str::SmolStr;
use std::sync::Arc;

use wp_model_core::model::{DataType, Value as ModelValue};

use super::*;

impl AlertColumnBuilder {
    /// L3 batched commit: append a whole segment of rows **column-major**, so
    /// the per-row fill that dominated Q1's on-each output (10 `Vec::push` +
    /// a `fill_row_gaps` scan per row) becomes 10 bulk `extend`s + one block-
    /// level yield fill. Byte-identical to committing each row via
    /// [`Self::commit_each_row`].
    ///
    /// `staged_rows` is one entry per row, in order, each a slice of that
    /// row's staged yield cells `(col_idx, meta, value)` (same layout
    /// `stage_yield_cell`/`stage_yield_cell_f64` produce).
    #[allow(clippy::too_many_arguments)]
    pub fn commit_each_rows_batch(
        &mut self,
        wfx_id: &[SmolStr],
        score: &[f64],
        entity_id: &[SmolStr],
        fired_at: &[String],
        rule_name: &Arc<str>,
        entity_type: &Arc<str>,
        origin: &Arc<str>,
        close_reason: &Arc<str>,
        emit_time: &Arc<str>,
        summary: &Arc<str>,
        staged_rows: &[Vec<(usize, DataType, ModelValue)>],
    ) {
        let n = wfx_id.len();
        debug_assert_eq!(score.len(), n);
        debug_assert_eq!(entity_id.len(), n);
        debug_assert_eq!(fired_at.len(), n);
        debug_assert_eq!(staged_rows.len(), n);
        // Reserve once for the whole block (amortizes the per-row growth).
        self.wfx_id.reserve(n);
        self.score.reserve(n);
        self.entity_id.reserve(n);
        self.fired_at.reserve(n);
        // 系统常量列免 per-row 数组——只 set 一次，不 reserve。
        // ⚠ 只 set 跨批安全的计划常量（rule_name/entity_type/origin/
        // close_reason）；emit_time/summary 走下方 Rows（跨批/规则语义变化）。
        Self::set_system_const(&mut self.rule_name, rule_name);
        Self::set_system_const(&mut self.entity_type, entity_type);
        Self::set_system_const(&mut self.origin, origin);
        Self::set_system_const(&mut self.close_reason, close_reason);
        // Bulk system columns.
        self.wfx_id.extend_from_slice(wfx_id);
        self.score.extend_from_slice(score);
        self.entity_id.extend_from_slice(entity_id);
        self.fired_at.extend_from_slice(fired_at);
        // 计划常量列：同一 Arc 值扩展 n 行（引用计数共享）——现改为列级
        // 常量（ColumnData::Const 一次存储），读时按行展开。
        // emit_time：跨批变化（cached_emit_time 按 nanos 缓存）→ Rows（R1）。
        match &mut self.emit_time {
            Some(ColumnData::Rows(v)) => v.extend(std::iter::repeat_n(Arc::clone(emit_time), n)),
            _ => self.emit_time = Some(ColumnData::Rows(vec![Arc::clone(emit_time); n])),
        }
        // summary：调用方（on-each 批式）传计划常量单值，但 match 语义可能
        // per-row——保守保持 Rows（n 行同一值，语义一致）。
        match &mut self.summary {
            Some(ColumnData::Rows(v)) => v.extend(std::iter::repeat_n(Arc::clone(summary), n)),
            _ => self.summary = Some(ColumnData::Rows(vec![Arc::clone(summary); n])),
        }
        // Yield cells, interleaved with per-row gap fills. `fill_row_gaps`
        // (per-row path) pushes one fill cell for every column that received no
        // staged cell that row — so a field that is present in rows {0, 2} but
        // absent in row 1 must land as [real0, fill, real2], NOT be topped up
        // to the block tail as a trailing run. To stay byte-identical we walk
        // the rows in order and, before pushing each real cell, pad that column
        // with the fill(s) for the rows since its previous real cell. Columns
        // with no staged cells (literal/idle) get the block-level top-up below.
        let target = self.len + n;
        for (row_idx, row) in staged_rows.iter().enumerate() {
            let row_pos = self.len + row_idx;
            for (col_idx, meta, value) in row {
                let col = &mut self.yield_cols[*col_idx];
                // Pad columns whose last real cell predates this row (field was
                // absent for intervening rows) — same fill value as
                // `fill_row_gaps` (batched constant, else Ignore/Null).
                // 常量列（const_value.is_some()）免每行 cell——staged_rows 里
                // 不会有其 cell（行循环 Lit continue 不 stage），无需处理。
                while col.values.len() < row_pos {
                    match &col.const_value {
                        Some((meta, value)) => {
                            col.metas.push(meta.clone());
                            col.values.push(value.clone());
                        }
                        None => {
                            col.metas.push(DataType::Ignore);
                            col.values.push(ModelValue::Null);
                        }
                    }
                }
                col.metas.push(meta.clone());
                col.values.push(value.clone());
            }
        }
        // Top up columns that never got a staged cell this block (literal /
        // idle columns) to the block target — each fill contributes the column
        // constant, else Ignore/Null (byte-identical to their per-row fills).
        // 常量列跳过：values 恒空，读时按行展开 const_value（免 ~32B/行 cell）。
        for col in &mut self.yield_cols {
            if col.is_const_column() {
                continue;
            }
            while col.values.len() < target {
                col.metas.push(DataType::Ignore);
                col.values.push(ModelValue::Null);
            }
        }
        debug_assert!(
            self.yield_cols
                .iter()
                .all(|c| c.is_const_column() || c.values.len() == target)
        );
        self.len += n;
    }

    /// L3 batched commit for the **close** path: identical to
    /// [`Self::commit_each_rows_batch`] except `origin`, `close_reason` and
    /// `summary` vary per row (close reason differs per close; the summary
    /// embeds the scope key). Same block-level yield fill semantics — must
    /// stay byte-identical to committing via [`Self::commit_each_row`] with
    /// per-record `AlertOrigin::Close` / `build_summary` outputs.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_close_rows_batch(
        &mut self,
        wfx_id: &[SmolStr],
        score: &[f64],
        entity_id: &[SmolStr],
        fired_at: &[String],
        rule_name: &Arc<str>,
        entity_type: &Arc<str>,
        origin: &[Arc<str>],
        close_reason: &[Arc<str>],
        emit_time: &Arc<str>,
        summary: &[Arc<str>],
        staged_rows: &[Vec<(usize, DataType, ModelValue)>],
    ) {
        let n = wfx_id.len();
        debug_assert_eq!(score.len(), n);
        debug_assert_eq!(entity_id.len(), n);
        debug_assert_eq!(fired_at.len(), n);
        debug_assert_eq!(origin.len(), n);
        debug_assert_eq!(close_reason.len(), n);
        debug_assert_eq!(summary.len(), n);
        debug_assert_eq!(staged_rows.len(), n);
        // Reserve once for the whole block (amortizes the per-row growth).
        self.wfx_id.reserve(n);
        self.score.reserve(n);
        self.entity_id.reserve(n);
        self.fired_at.reserve(n);
        // 系统列：rule_name/entity_type 批级常量 → Const；emit_time 跨批变化
        //（cached_emit_time 按 nanos）→ Rows（R1）；origin/close_reason/
        // summary per-row（close 逐条变化）→ Rows。
        Self::set_system_const(&mut self.rule_name, rule_name);
        Self::set_system_const(&mut self.entity_type, entity_type);
        match &mut self.emit_time {
            Some(ColumnData::Rows(v)) => v.extend(std::iter::repeat_n(Arc::clone(emit_time), n)),
            _ => self.emit_time = Some(ColumnData::Rows(vec![Arc::clone(emit_time); n])),
        }
        match &mut self.origin {
            Some(ColumnData::Rows(v)) => v.extend_from_slice(origin),
            _ => self.origin = Some(ColumnData::Rows(origin.to_vec())),
        }
        match &mut self.close_reason {
            Some(ColumnData::Rows(v)) => v.extend_from_slice(close_reason),
            _ => self.close_reason = Some(ColumnData::Rows(close_reason.to_vec())),
        }
        match &mut self.summary {
            Some(ColumnData::Rows(v)) => v.extend_from_slice(summary),
            _ => self.summary = Some(ColumnData::Rows(summary.to_vec())),
        }
        // Bulk system columns. Plan-constant columns: same `Arc` every row.
        self.wfx_id.extend_from_slice(wfx_id);
        self.score.extend_from_slice(score);
        self.entity_id.extend_from_slice(entity_id);
        self.fired_at.extend_from_slice(fired_at);
        // Yield cells, interleaved with per-row gap fills — see
        // `commit_each_rows_batch` for the byte-identity contract.
        let target = self.len + n;
        for (row_idx, row) in staged_rows.iter().enumerate() {
            let row_pos = self.len + row_idx;
            for (col_idx, meta, value) in row {
                let col = &mut self.yield_cols[*col_idx];
                while col.values.len() < row_pos {
                    match &col.const_value {
                        Some((meta, value)) => {
                            col.metas.push(meta.clone());
                            col.values.push(value.clone());
                        }
                        None => {
                            col.metas.push(DataType::Ignore);
                            col.values.push(ModelValue::Null);
                        }
                    }
                }
                col.metas.push(meta.clone());
                col.values.push(value.clone());
            }
        }
        for col in &mut self.yield_cols {
            while col.values.len() < target {
                match &col.const_value {
                    Some((meta, value)) => {
                        col.metas.push(meta.clone());
                        col.values.push(value.clone());
                    }
                    None => {
                        col.metas.push(DataType::Ignore);
                        col.values.push(ModelValue::Null);
                    }
                }
            }
        }
        debug_assert!(self.yield_cols.iter().all(|c| c.values.len() == target));
        self.len += n;
    }

    /// Seal the builder into an immutable batch. The builder is left empty
    /// (capacities are not preserved; see flush call sites for reuse) and its
    /// layout cache is dropped — the yield columns moved out invalidate the
    /// cached column indices, so a reused builder must re-resolve them.
    pub fn finish(&mut self) -> AlertColumnBatch {
        self.layout_cache.clear();
        AlertColumnBatch {
            target: Arc::clone(&self.target),
            len: std::mem::take(&mut self.len),
            wfx_id: std::mem::take(&mut self.wfx_id),
            rule_name: self
                .rule_name
                .take()
                .unwrap_or(ColumnData::Rows(Vec::new())),
            score: std::mem::take(&mut self.score),
            entity_type: self
                .entity_type
                .take()
                .unwrap_or(ColumnData::Rows(Vec::new())),
            entity_id: std::mem::take(&mut self.entity_id),
            origin: self.origin.take().unwrap_or(ColumnData::Rows(Vec::new())),
            close_reason: self
                .close_reason
                .take()
                .unwrap_or(ColumnData::Rows(Vec::new())),
            fired_at: std::mem::take(&mut self.fired_at),
            emit_time: self
                .emit_time
                .take()
                .unwrap_or(ColumnData::Rows(Vec::new())),
            summary: self.summary.take().unwrap_or(ColumnData::Rows(Vec::new())),
            yield_cols: std::mem::take(&mut self.yield_cols),
        }
    }
}
