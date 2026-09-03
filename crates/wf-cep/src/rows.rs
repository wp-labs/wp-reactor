//! 行字段紧凑存储（P4-A 片 3）：`RowFieldLayout` / `RowFields` / `RowFieldSlot`。
//!
//! stats last/top 的行字段列数组：数字/字符串/其它三槽 + null 位掩码。
//! `from_schema` 需 arrow 纯数据类型（墙内允许）；不触 IO/async。

use crate::value::Value;

/// 行字段槽型（2026-08-26 q18/q19：stats last/top 行字段紧凑化）。
/// 每字段一个槽位：数字→`numeric`（f64 8B）、字符串→`strings`（SmolStr 24B
/// 内联）、其它→`others`（原 `Option<Value>` 万能盒回退）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.StatsEngine")]
pub enum RowFieldSlot {
    Numeric(usize),
    Str(usize),
    Other(usize),
}

/// 字段类型分派表（executor 级，所有桶共享；列式从 batch schema 构建，
/// 行式无静态类型时退化为全 Other——不紧凑但正确）。
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct RowFieldLayout {
    slots: Vec<RowFieldSlot>,
    n_numeric: usize,
    n_strings: usize,
    n_others: usize,
}

impl RowFieldLayout {
    /// 从 batch schema 构建（列式路径：字段类型静态已知）。
    /// `names` = 行字段列序（P5 子集，或全部 schema 字段排序）。
    pub fn from_schema(names: &[String], schema: &arrow::datatypes::Schema) -> Self {
        let mut slots = Vec::with_capacity(names.len());
        let (mut n_num, mut n_str, mut n_oth) = (0, 0, 0);
        for name in names {
            let slot = match schema.column_with_name(name).map(|(_, f)| f.data_type()) {
                Some(arrow::datatypes::DataType::Int8)
                | Some(arrow::datatypes::DataType::Int16)
                | Some(arrow::datatypes::DataType::Int32)
                | Some(arrow::datatypes::DataType::Int64)
                | Some(arrow::datatypes::DataType::UInt8)
                | Some(arrow::datatypes::DataType::UInt16)
                | Some(arrow::datatypes::DataType::UInt32)
                | Some(arrow::datatypes::DataType::UInt64)
                | Some(arrow::datatypes::DataType::Float32)
                | Some(arrow::datatypes::DataType::Float64)
                | Some(arrow::datatypes::DataType::Timestamp(_, _)) => {
                    let s = RowFieldSlot::Numeric(n_num);
                    n_num += 1;
                    s
                }
                Some(arrow::datatypes::DataType::Utf8)
                | Some(arrow::datatypes::DataType::LargeUtf8) => {
                    let s = RowFieldSlot::Str(n_str);
                    n_str += 1;
                    s
                }
                _ => {
                    let s = RowFieldSlot::Other(n_oth);
                    n_oth += 1;
                    s
                }
            };
            slots.push(slot);
        }
        Self {
            slots,
            n_numeric: n_num,
            n_strings: n_str,
            n_others: n_oth,
        }
    }

    /// 全 Other 兜底（行式路径无静态 schema 类型时；不紧凑但语义一致）。
    pub fn all_other(names: &[String]) -> Self {
        Self {
            slots: names
                .iter()
                .enumerate()
                .map(|(i, _)| RowFieldSlot::Other(i))
                .collect(),
            n_numeric: 0,
            n_strings: 0,
            n_others: names.len(),
        }
    }

    pub fn n_fields(&self) -> usize {
        self.slots.len()
    }

    pub fn n_numeric(&self) -> usize {
        self.n_numeric
    }

    pub fn n_strings(&self) -> usize {
        self.n_strings
    }

    pub fn n_others(&self) -> usize {
        self.n_others
    }

    pub fn slot(&self, i: usize) -> RowFieldSlot {
        self.slots[i]
    }
}

/// 行字段紧凑存储（stats last/top 的行字段数组）。
/// `Arc<[Option<Value>]>`（56B/字段）→ 按 [`RowFieldLayout`] 槽分派：
/// 数字 8B / 字符串 24B（内联）/ 其它回退。null 由 `null_mask` 位标记
/// （numeric 的 NaN 与 strings 的空串都是合法数据，不能作哨兵）。
/// 自包含 layout（Arc），下游（stats_task 注入）可独立读取。
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct RowFields {
    layout: std::sync::Arc<RowFieldLayout>,
    numeric: Box<[f64]>,
    strings: Box<[smol_str::SmolStr]>,
    others: Box<[Option<Value>]>,
    null_mask: Box<[u64]>,
}

impl RowFields {
    pub fn empty(layout: std::sync::Arc<RowFieldLayout>) -> Self {
        let n = layout.n_fields();
        let n_numeric = layout.n_numeric;
        let n_strings = layout.n_strings;
        let n_others = layout.n_others;
        Self {
            layout,
            numeric: vec![0.0; n_numeric].into_boxed_slice(),
            strings: vec![smol_str::SmolStr::default(); n_strings].into_boxed_slice(),
            others: vec![None; n_others].into_boxed_slice(),
            null_mask: vec![0u64; n.div_ceil(64)].into_boxed_slice(),
        }
    }

    pub fn layout(&self) -> &std::sync::Arc<RowFieldLayout> {
        &self.layout
    }

    fn mask_bit(&mut self, i: usize, is_null: bool) {
        let word = i / 64;
        let bit = i % 64;
        if is_null {
            self.null_mask[word] |= 1 << bit;
        } else {
            self.null_mask[word] &= !(1 << bit);
        }
    }

    fn mask_get(&self, i: usize) -> bool {
        (self.null_mask[i / 64] >> (i % 64)) & 1 == 1
    }

    /// 按字段位置写值（v = None → null）。
    pub fn set(&mut self, i: usize, v: Option<Value>) {
        match (self.layout.slot(i), v) {
            (RowFieldSlot::Numeric(idx), Some(Value::Number(n))) => {
                self.numeric[idx] = n;
                self.mask_bit(i, false);
            }
            (RowFieldSlot::Str(idx), Some(Value::Str(s))) => {
                self.strings[idx] = s;
                self.mask_bit(i, false);
            }
            (RowFieldSlot::Other(idx), Some(v)) => {
                self.others[idx] = Some(v);
                self.mask_bit(i, false);
            }
            (_, None) => {
                self.mask_bit(i, true);
            }
            // 值类型与槽型不符（行式路径按值路由的边界）→ null（与提取失败一致）。
            (_, Some(_)) => {
                self.mask_bit(i, true);
            }
        }
    }

    /// 按字段位置读值（null → None）。
    pub fn value_at(&self, i: usize) -> Option<Value> {
        if self.mask_get(i) {
            return None;
        }
        match self.layout.slot(i) {
            RowFieldSlot::Numeric(idx) => Some(Value::Number(self.numeric[idx])),
            RowFieldSlot::Str(idx) => Some(Value::Str(self.strings[idx].clone())),
            RowFieldSlot::Other(idx) => self.others[idx].clone(),
        }
    }

    /// 按字段位置读数字（top 排序键 / last measure_value）。
    pub fn f64_at(&self, i: usize) -> Option<f64> {
        if self.mask_get(i) {
            return None;
        }
        match self.layout.slot(i) {
            RowFieldSlot::Numeric(idx) => Some(self.numeric[idx]),
            RowFieldSlot::Other(idx) => self.others[idx].as_ref().and_then(value_to_f64),
            RowFieldSlot::Str(_) => None,
        }
    }

    /// 按字段位置迭代（下游 field_values 注入用；与 `Arc<[Option<Value>]>`
    /// 的 iter 同构）。
    pub fn iter_values(&self) -> impl Iterator<Item = Option<Value>> + '_ {
        (0..self.layout.n_fields()).map(move |i| self.value_at(i))
    }

    // -- spill 序列化访问器（pub(crate)：仅 wf-engine 内部 spill 模块使用）--

    /// 数字槽数组（layout 槽序）。
    pub fn numeric(&self) -> &[f64] {
        &self.numeric
    }

    /// 字符串槽数组（layout 槽序）。
    pub fn strings(&self) -> &[smol_str::SmolStr] {
        &self.strings
    }

    /// 其它槽数组（layout 槽序）。
    pub fn others(&self) -> &[Option<Value>] {
        &self.others
    }

    /// null 位掩码（layout 槽序，位 1 = null）。
    pub fn null_mask(&self) -> &[u64] {
        &self.null_mask
    }

    /// 从槽数组构造（spill 读回；槽序与 [`Self::empty`] 一致，布局由
    /// `layout` 描述——序列化不落 layout，读回按当前 executor 的 layout 解释）。
    pub fn from_parts(
        layout: std::sync::Arc<RowFieldLayout>,
        numeric: Box<[f64]>,
        strings: Box<[smol_str::SmolStr]>,
        others: Box<[Option<Value>]>,
        null_mask: Box<[u64]>,
    ) -> Self {
        Self {
            layout,
            numeric,
            strings,
            others,
            null_mask,
        }
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}
