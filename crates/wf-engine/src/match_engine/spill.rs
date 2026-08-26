//! stats 状态 spill 存储（2026-08-26 设计，见 `docs/design/stats-state-spill-redb.md`）。
//!
//! ## 分层
//! - [`SpillStore`]：外溢存储抽象（trait）。hot path 只调 [`SpillStore::contains`]
//!   （O(1) 内存操作，不碰磁盘）；put/get 是低频（LRU 驱逐 / spill 键回访）。
//! - [`NoopSpillStore`]：默认（未配置 spill）——`contains` 恒 false，put/get/drain
//!   空操作，hot path 一个分支预测，零开销。
//! - `RedbSpillStore`：redb 持久化（M2 实现，本文件仅 trait + Noop + 序列化）。
//!
//! ## 序列化（手写字节编码，非 serde）
//! - [`ScopeKey`] 编码与 `scope_key_hash` 的字节序同构（tag + payload），
//!   round-trip 对拍保证与 `comps_match` / `scope_key_from_comps` 一致。
//! - [`StatsAccum`] 按变体 tag 分派；[`RowFields`] 按 layout 槽序写数组
//!   （**layout 不序列化**——读回时按当前 executor 的 layout 解释，同一
//!   executor 生命周期内不变，成立）。
//!
//! ## 正确性红线
//! 反序列化遇损坏数据 → 返回 `Err(SpillError::Corrupt)`（调用方 panic，绝不
//! 静默丢键）。长度字段带上限校验（防恶意/损坏长度导致 OOM）。

use crate::match_engine::executor::{
    DistinctKey, DistinctSet, NumericAccum, RowFields, StatsAccum, TopEntry,
};
use crate::match_engine::ScopeKey;

/// spill 存储错误。
#[derive(Debug)]
pub enum SpillError {
    /// 反序列化损坏（长度越界 / 未知 tag / 截断）——致命，调用方须 panic。
    Corrupt(String),
    /// 状态含 spill 不支持的形态（如 last 行的结构化 Array/Object 值）——
    /// 致命（显式拒绝，绝不静默改写）。
    Unsupported(String),
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpillError::Corrupt(msg) => write!(f, "spill 数据损坏: {msg}"),
            SpillError::Unsupported(msg) => write!(f, "spill 不支持: {msg}"),
        }
    }
}

impl std::error::Error for SpillError {}

/// 状态外溢存储抽象（见模块文档）。
pub trait SpillStore {
    /// 键是否已 spill（hot path 存在性检查，O(1) 内存操作）。
    fn contains(&self, hash: u64) -> bool;

    /// spill 一个键（持久层写入；buckets 中已移除）。
    fn put(&mut self, hash: u64, key: &ScopeKey, accs: Vec<StatsAccum>) -> Result<(), SpillError>;

    /// 读回一个键（低频：spill 后键又来一条）。
    fn get(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)>;

    /// close：读回全部 spill 键（顺序无要求，调用方排序）。
    fn drain(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)>;

    /// 当前已 spill 键数（诊断/指标）。
    fn len(&self) -> usize;

    /// 是否无 spill 键（默认实现：`len() == 0`）。
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 默认空实现：未配置 spill 时零开销。
#[derive(Default)]
pub struct NoopSpillStore;

impl SpillStore for NoopSpillStore {
    fn contains(&self, _hash: u64) -> bool {
        false
    }
    fn put(&mut self, _hash: u64, _key: &ScopeKey, _accs: Vec<StatsAccum>) -> Result<(), SpillError> {
        Ok(())
    }
    fn get(&mut self, _hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)> {
        None
    }
    fn drain(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        Vec::new()
    }
    fn len(&self) -> usize {
        0
    }
}

/// 内存 spill 目录（M2 redb 之前的最小可用版）：HashMap<hash, (ScopeKey, accs)>。
/// 用于对拍/测试（与 redb 行为等价，纯内存）。
#[derive(Default)]
pub struct MemSpillStore {
    map: std::collections::HashMap<u64, (ScopeKey, Vec<StatsAccum>)>,
}

impl MemSpillStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SpillStore for MemSpillStore {
    fn contains(&self, hash: u64) -> bool {
        self.map.contains_key(&hash)
    }
    fn put(&mut self, hash: u64, key: &ScopeKey, accs: Vec<StatsAccum>) -> Result<(), SpillError> {
        self.map.insert(hash, (key.clone(), accs));
        Ok(())
    }
    fn get(&mut self, hash: u64) -> Option<(ScopeKey, Vec<StatsAccum>)> {
        self.map.get(&hash).map(|(k, a)| (k.clone(), a.clone()))
    }
    fn drain(&mut self) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        std::mem::take(&mut self.map)
            .into_values()
            .collect::<Vec<_>>()
    }
    fn len(&self) -> usize {
        self.map.len()
    }
}

// ---------------------------------------------------------------------------
// 字节写入器/读取器（小端，长度前缀带上限）
// ---------------------------------------------------------------------------

/// 单键/单桶的序列化长度上限（防护：损坏长度导致 OOM）。ScopeKey 树 8 层、
/// accs 16 度量、行字段 64 字段的合理上界 ~1MB。
const MAX_SERIALIZED_BYTES: usize = 1 << 20;

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn i128(&mut self, v: i128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_bits().to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn u8(&mut self) -> Result<u8, SpillError> {
        let v = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| SpillError::Corrupt("u8 越界".into()))?;
        self.pos += 1;
        Ok(v)
    }
    fn u64(&mut self) -> Result<u64, SpillError> {
        let end = self
            .pos
            .checked_add(8)
            .ok_or_else(|| SpillError::Corrupt("u64 长度溢出".into()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| SpillError::Corrupt("u64 越界".into()))?;
        self.pos = end;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, SpillError> {
        Ok(self.u64()? as i64)
    }
    fn i128(&mut self) -> Result<i128, SpillError> {
        let end = self
            .pos
            .checked_add(16)
            .ok_or_else(|| SpillError::Corrupt("i128 长度溢出".into()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| SpillError::Corrupt("i128 越界".into()))?;
        self.pos = end;
        Ok(i128::from_le_bytes(bytes.try_into().unwrap()))
    }
    fn f64(&mut self) -> Result<f64, SpillError> {
        Ok(f64::from_bits(self.u64()?))
    }
    fn bytes(&mut self) -> Result<&'a [u8], SpillError> {
        let len = self.u64()? as usize;
        if len > MAX_SERIALIZED_BYTES {
            return Err(SpillError::Corrupt(format!("bytes 长度 {len} 超上限")));
        }
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| SpillError::Corrupt("bytes 长度溢出".into()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| SpillError::Corrupt("bytes 越界".into()))?;
        self.pos = end;
        Ok(bytes)
    }
}

// ---------------------------------------------------------------------------
// ScopeKey 序列化
// ---------------------------------------------------------------------------

/// tag 常量（与 `scope_key_hash` 的 tag 同构：Empty=0 Int=1 Float=2 Str=3 Pair=4）。
const TAG_EMPTY: u8 = 0;
const TAG_INT: u8 = 1;
const TAG_FLOAT: u8 = 2;
const TAG_STR: u8 = 3;
const TAG_PAIR: u8 = 4;

/// 递归编码（字节序与 `scope_key_hash` 同构——`comps_hash` 镜像）。
/// 嵌套深度限 [`MAX_SCOPE_KEY_DEPTH`]（防损坏数据超深 Pair 递归栈溢出）。
fn write_scope_key(w: &mut Writer, key: &ScopeKey) {
    match key {
        ScopeKey::Empty => w.u8(TAG_EMPTY),
        ScopeKey::Int(v) => {
            w.u8(TAG_INT);
            w.i64(*v);
        }
        ScopeKey::Float(bits) => {
            w.u8(TAG_FLOAT);
            w.u64(*bits);
        }
        ScopeKey::Str(s) => {
            w.u8(TAG_STR);
            w.bytes(s.as_bytes());
        }
        ScopeKey::Pair(a, b) => {
            w.u8(TAG_PAIR);
            write_scope_key(w, a);
            write_scope_key(w, b);
        }
    }
}

/// ScopeKey 树嵌套深度上限（正常键组合 ~8 层；深度超限 = 损坏）。
const MAX_SCOPE_KEY_DEPTH: usize = 32;

fn read_scope_key(r: &mut Reader<'_>) -> Result<ScopeKey, SpillError> {
    read_scope_key_depth(r, 0)
}

fn read_scope_key_depth(r: &mut Reader<'_>, depth: usize) -> Result<ScopeKey, SpillError> {
    if depth > MAX_SCOPE_KEY_DEPTH {
        return Err(SpillError::Corrupt("ScopeKey 嵌套过深".into()));
    }
    match r.u8()? {
        TAG_EMPTY => Ok(ScopeKey::Empty),
        TAG_INT => Ok(ScopeKey::Int(r.i64()?)),
        TAG_FLOAT => Ok(ScopeKey::Float(r.u64()?)),
        TAG_STR => {
            let s = r.bytes()?;
            let s = std::str::from_utf8(s)
                .map_err(|_| SpillError::Corrupt("ScopeKey Str 非 UTF-8".into()))?;
            Ok(ScopeKey::Str(s.into()))
        }
        TAG_PAIR => {
            let a = read_scope_key_depth(r, depth + 1)?;
            let b = read_scope_key_depth(r, depth + 1)?;
            Ok(ScopeKey::Pair(Box::new(a), Box::new(b)))
        }
        other => Err(SpillError::Corrupt(format!("ScopeKey 未知 tag {other}"))),
    }
}

// ---------------------------------------------------------------------------
// StatsAccum 序列化
// ---------------------------------------------------------------------------

/// StatsAccum 变体 tag。
const TAG_NUMERIC: u8 = 0;
const TAG_DISTINCT: u8 = 1;
const TAG_LAST: u8 = 2;
const TAG_TOP: u8 = 3;

/// RowFields 序列化：按 layout 槽序写数组（layout 不序列化——读回时外部传入）。
/// 写：numeric（f64×n）→ strings（bytes×n，SmolStr）→ others（tag+payload）→ null_mask。
fn write_row_fields(w: &mut Writer, rf: &RowFields) -> Result<(), SpillError> {
    let layout = rf.layout();
    w.u64(layout.n_fields() as u64);
    // 直接读内部数组（layout 槽序，与 value_at 口径一致；访问器 pub(crate) 同 crate）。
    for v in rf.numeric() {
        w.f64(*v);
    }
    for s in rf.strings() {
        w.bytes(s.as_bytes());
    }
    for v in rf.others() {
        match v {
            None => w.u8(0),
            Some(v) => {
                w.u8(1);
                write_value(w, v)?;
            }
        }
    }
    for m in rf.null_mask() {
        w.u64(*m);
    }
    Ok(())
}

/// Value 序列化（RowFields.others 的 `Option<Value>` 用）。
/// Array/Object 结构化值拒绝 spill（否则读回空值 = 静默丢数据）。
fn write_value(w: &mut Writer, v: &crate::match_engine::Value) -> Result<(), SpillError> {
    match v {
        crate::match_engine::Value::Number(n) => {
            w.u8(0);
            w.f64(*n);
            Ok(())
        }
        crate::match_engine::Value::Str(s) => {
            w.u8(1);
            w.bytes(s.as_bytes());
            Ok(())
        }
        crate::match_engine::Value::Bool(b) => {
            w.u8(2);
            w.u8(*b as u8);
            Ok(())
        }
        crate::match_engine::Value::Array(_) => Err(SpillError::Unsupported(
            "RowFields others 含 Array 值".into(),
        )),
        crate::match_engine::Value::Object(_) => Err(SpillError::Unsupported(
            "RowFields others 含 Object 值".into(),
        )),
    }
}

fn read_value(r: &mut Reader<'_>) -> Result<crate::match_engine::Value, SpillError> {
    Ok(match r.u8()? {
        0 => crate::match_engine::Value::Number(r.f64()?),
        1 => {
            let s = r.bytes()?;
            crate::match_engine::Value::Str(
                std::str::from_utf8(s)
                    .map_err(|_| SpillError::Corrupt("Value Str 非 UTF-8".into()))?
                    .into(),
            )
        }
        2 => crate::match_engine::Value::Bool(r.u8()? != 0),
        3 => crate::match_engine::Value::Array(Vec::new()),
        4 => crate::match_engine::Value::Object(Default::default()),
        other => return Err(SpillError::Corrupt(format!("Value 未知 tag {other}"))),
    })
}

/// DistinctKey 序列化。
fn write_distinct_key(w: &mut Writer, k: &DistinctKey) {
    match k {
        DistinctKey::Int(v) => {
            w.u8(0);
            w.i64(*v);
        }
        DistinctKey::Float(bits) => {
            w.u8(1);
            w.u64(*bits);
        }
        DistinctKey::Str(s) => {
            w.u8(2);
            w.bytes(s.as_bytes());
        }
    }
}

fn read_distinct_key(r: &mut Reader<'_>) -> Result<DistinctKey, SpillError> {
    Ok(match r.u8()? {
        0 => DistinctKey::Int(r.i64()?),
        1 => DistinctKey::Float(r.u64()?),
        2 => DistinctKey::Str(
            std::str::from_utf8(r.bytes()?)
                .map_err(|_| SpillError::Corrupt("DistinctKey Str 非 UTF-8".into()))?
                .into(),
        ),
        other => return Err(SpillError::Corrupt(format!("DistinctKey 未知 tag {other}"))),
    })
}

/// 序列化 accs 数组 + 每桶行字段 layout（写时随 accs 写 layout 描述）。
/// 返回 (编码字节, 写时 layout 的字段名序) —— 读回时若 layout 不一致需重建。
pub fn serialize_accs(accs: &[StatsAccum]) -> Result<Vec<u8>, SpillError> {
    let mut w = Writer::new();
    w.u64(accs.len() as u64);
    for acc in accs {
        match acc {
            StatsAccum::Numeric(n) => {
                w.u8(TAG_NUMERIC);
                w.u64(n.count);
                // sum/min/max 为 i128（可超 i64）——全宽写，读回无截断。
                w.i128(n.sum);
                match n.min {
                    Some(m) => {
                        w.u8(1);
                        w.i128(m);
                    }
                    None => w.u8(0),
                }
                match n.max {
                    Some(m) => {
                        w.u8(1);
                        w.i128(m);
                    }
                    None => w.u8(0),
                }
            }
            StatsAccum::Distinct(d) => {
                w.u8(TAG_DISTINCT);
                // ints 集合
                let ints: Vec<i64> = d.ints().iter().copied().collect();
                w.u64(ints.len() as u64);
                for v in ints {
                    w.i64(v);
                }
                // others 集合
                let others: Vec<&DistinctKey> = d.others().iter().collect();
                w.u64(others.len() as u64);
                for k in others {
                    write_distinct_key(&mut w, k);
                }
            }
            StatsAccum::Last(rf) => {
                w.u8(TAG_LAST);
                match rf {
                    Some(rf) => {
                        w.u8(1);
                        write_row_fields(&mut w, rf)?;
                    }
                    None => w.u8(0),
                }
            }
            StatsAccum::Top(entries) => {
                w.u8(TAG_TOP);
                w.u64(entries.len() as u64);
                for e in entries {
                    w.f64(e.key);
                    write_row_fields(&mut w, &e.row)?;
                }
            }
        }
    }
    if w.buf.len() > MAX_SERIALIZED_BYTES {
        return Err(SpillError::Corrupt(format!(
            "accs 序列化超上限 {}B",
            w.buf.len()
        )));
    }
    Ok(w.finish())
}

/// 反序列化 accs 数组。`layout` = 当前 executor 的 RowFieldLayout（读回
/// RowFields 按此解释；若与写时 layout 不一致 → Corrupt）。
pub fn deserialize_accs(
    bytes: &[u8],
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<Vec<StatsAccum>, SpillError> {
    let mut r = Reader::new(bytes);
    let n = r.u64()? as usize;
    if n > 1024 {
        return Err(SpillError::Corrupt(format!("accs 数量 {n} 超上限")));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let acc = match r.u8()? {
            TAG_NUMERIC => {
                let count = r.u64()?;
                let sum = r.i128()?;
                let min = if r.u8()? == 1 {
                    Some(r.i128()?)
                } else {
                    None
                };
                let max = if r.u8()? == 1 {
                    Some(r.i128()?)
                } else {
                    None
                };
                StatsAccum::Numeric(Box::new(NumericAccum {
                    count,
                    sum,
                    min,
                    max,
                }))
            }
            TAG_DISTINCT => {
                let n_ints = r.u64()? as usize;
                if n_ints > MAX_SERIALIZED_BYTES / 8 {
                    return Err(SpillError::Corrupt("distinct ints 超上限".into()));
                }
                let mut ints = crate::match_engine::EngineHashSet::default();
                for _ in 0..n_ints {
                    ints.insert(r.i64()?);
                }
                let n_others = r.u64()? as usize;
                if n_others > MAX_SERIALIZED_BYTES / 8 {
                    return Err(SpillError::Corrupt("distinct others 超上限".into()));
                }
                let mut others = crate::match_engine::EngineHashSet::default();
                for _ in 0..n_others {
                    others.insert(read_distinct_key(&mut r)?);
                }
                StatsAccum::Distinct(Box::new(DistinctSet::from_parts(ints, others)))
            }
            TAG_LAST => {
                let rf = if r.u8()? == 1 {
                    Some(read_row_fields_with_layout(&mut r, layout)?)
                } else {
                    None
                };
                StatsAccum::Last(rf.map(std::sync::Arc::new))
            }
            TAG_TOP => {
                let n = r.u64()? as usize;
                if n > MAX_SERIALIZED_BYTES / 64 {
                    return Err(SpillError::Corrupt("top 条目超上限".into()));
                }
                let mut entries = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = r.f64()?;
                    let row = read_row_fields_with_layout(&mut r, layout)?;
                    entries.push(TopEntry { key, row });
                }
                StatsAccum::Top(entries)
            }
            other => return Err(SpillError::Corrupt(format!("StatsAccum 未知 tag {other}"))),
        };
        out.push(acc);
    }
    Ok(out)
}

/// RowFields 反序列化（带 layout 版）。
fn read_row_fields_with_layout(
    r: &mut Reader<'_>,
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<RowFields, SpillError> {
    let n_fields = r.u64()? as usize;
    if n_fields != layout.n_fields() {
        return Err(SpillError::Corrupt(format!(
            "RowFields 字段数 {n_fields} != layout {}",
            layout.n_fields()
        )));
    }
    let mut numeric = Vec::with_capacity(layout.n_numeric());
    for _ in 0..layout.n_numeric() {
        numeric.push(r.f64()?);
    }
    let mut strings = Vec::with_capacity(layout.n_strings());
    for _ in 0..layout.n_strings() {
        let s = r.bytes()?;
        strings.push(
            std::str::from_utf8(s)
                .map_err(|_| SpillError::Corrupt("RowFields Str 非 UTF-8".into()))?
                .into(),
        );
    }
    let mut others = Vec::with_capacity(layout.n_others());
    for _ in 0..layout.n_others() {
        others.push(if r.u8()? == 1 {
            Some(read_value(r)?)
        } else {
            None
        });
    }
    let n_words = n_fields.div_ceil(64);
    let mut null_mask = Vec::with_capacity(n_words);
    for _ in 0..n_words {
        null_mask.push(r.u64()?);
    }
    Ok(RowFields::from_parts(
        std::sync::Arc::clone(layout),
        numeric.into_boxed_slice(),
        strings.into_boxed_slice(),
        others.into_boxed_slice(),
        null_mask.into_boxed_slice(),
    ))
}

/// ScopeKey 序列化。
pub fn serialize_scope_key(key: &ScopeKey) -> Vec<u8> {
    let mut w = Writer::new();
    write_scope_key(&mut w, key);
    w.finish()
}

/// ScopeKey 反序列化。
pub fn deserialize_scope_key(bytes: &[u8]) -> Result<ScopeKey, SpillError> {
    let mut r = Reader::new(bytes);
    let key = read_scope_key(&mut r)?;
    // 尾部不应有残留（严格性：长度不符 = 损坏）。
    if r.pos != bytes.len() {
        return Err(SpillError::Corrupt("ScopeKey 序列化尾部残留".into()));
    }
    Ok(key)
}

/// 便捷：完整 spill 值（key + accs）编码（redb value = 此字节）。
pub fn serialize_spill_value(key: &ScopeKey, accs: &[StatsAccum]) -> Result<Vec<u8>, SpillError> {
    let mut w = Writer::new();
    write_scope_key(&mut w, key);
    let accs_bytes = serialize_accs(accs)?;
    w.bytes(&accs_bytes);
    Ok(w.finish())
}

/// 便捷：完整 spill 值解码。
pub fn deserialize_spill_value(
    bytes: &[u8],
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<(ScopeKey, Vec<StatsAccum>), SpillError> {
    let mut r = Reader::new(bytes);
    let key = read_scope_key(&mut r)?;
    let accs_bytes = r.bytes()?;
    let accs = deserialize_accs(accs_bytes, layout)?;
    // 尾部不应有残留（严格性：长度不符 = 损坏）。
    if r.pos != bytes.len() {
        return Err(SpillError::Corrupt("spill value 序列化尾部残留".into()));
    }
    Ok((key, accs))
}

/// 便捷：单桶 hash（spill key 用 `scope_key_hash` 同值）。
pub fn spill_hash(key: &ScopeKey) -> u64 {
    crate::match_engine::executor::scope_key_hash(key)
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_engine::executor::RowFieldLayout;

    fn sample_layout() -> std::sync::Arc<RowFieldLayout> {
        // numeric: price/dateTime；str: channel/url；other: 1 个。
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new(
                "dateTime",
                arrow::datatypes::DataType::Int64,
                false,
            ),
            arrow::datatypes::Field::new("channel", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("url", arrow::datatypes::DataType::Utf8, false),
        ]);
        std::sync::Arc::new(RowFieldLayout::from_schema(
            &["price", "dateTime", "channel", "url"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            &schema,
        ))
    }

    #[test]
    fn scope_key_roundtrip_all_variants() {
        let keys = [
            ScopeKey::Empty,
            ScopeKey::Int(42),
            ScopeKey::Int(-7),
            ScopeKey::Float(1234.5f64.to_bits()),
            ScopeKey::Str("hello".into()),
            ScopeKey::Pair(
                Box::new(ScopeKey::Int(1)),
                Box::new(ScopeKey::Str("a".into())),
            ),
            ScopeKey::Pair(
                Box::new(ScopeKey::Pair(
                    Box::new(ScopeKey::Int(1)),
                    Box::new(ScopeKey::Int(2)),
                )),
                Box::new(ScopeKey::Str("深".into())),
            ),
        ];
        for k in &keys {
            let bytes = serialize_scope_key(k);
            let back = deserialize_scope_key(&bytes).expect("roundtrip");
            assert_eq!(&back, k, "ScopeKey roundtrip 不一致: {k:?}");
        }
    }

    #[test]
    fn scope_key_corrupt_rejected() {
        // 未知 tag
        assert!(matches!(
            deserialize_scope_key(&[99]),
            Err(SpillError::Corrupt(_))
        ));
        // 截断（Int 缺 payload）
        assert!(matches!(
            deserialize_scope_key(&[TAG_INT]),
            Err(SpillError::Corrupt(_))
        ));
        // 尾部残留
        let bytes = serialize_scope_key(&ScopeKey::Int(1));
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(matches!(
            deserialize_scope_key(&bad),
            Err(SpillError::Corrupt(_))
        ));
    }

    #[test]
    fn scope_key_deep_pair_nesting_rejected() {
        // 深度超限（构造 64 层 Pair）→ Corrupt（非栈溢出）
        let mut bytes = vec![TAG_PAIR; 64];
        bytes.push(TAG_EMPTY);
        bytes.push(TAG_EMPTY);
        assert!(matches!(
            deserialize_scope_key(&bytes),
            Err(SpillError::Corrupt(msg)) if msg.contains("嵌套过深")
        ));
    }

    #[test]
    fn numeric_accum_i128_wide_roundtrip() {
        // sum/min/max 超 i64 范围（1<<70 ≈ 1.18e21 > i64::MAX ≈ 9.2e18）——
        // 全宽往返，无截断。
        let layout = sample_layout();
        let accs = vec![StatsAccum::Numeric(Box::new(NumericAccum {
            count: 3,
            sum: (1i128 << 70) + 12345,
            min: Some(-(1i128 << 65) - 7),
            max: Some((1i128 << 66) + 999),
        }))];
        let bytes = serialize_accs(&accs).expect("serialize");
        let back = deserialize_accs(&bytes, &layout).expect("deserialize");
        let n = back[0].numeric();
        assert_eq!(n.count, 3);
        assert_eq!(n.sum, (1i128 << 70) + 12345);
        assert_eq!(n.min, Some(-(1i128 << 65) - 7));
        assert_eq!(n.max, Some((1i128 << 66) + 999));
    }

    #[test]
    fn structured_value_in_last_rejected_not_silently_dropped() {
        // Boolean 字段在 from_schema 中路由到 others 槽——Array 值若出现
        // 必须显式拒绝（Unsupported），不能静默改写成空值。
        let bool_layout = std::sync::Arc::new(RowFieldLayout::from_schema(
            &["flag".to_string()],
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "flag",
                arrow::datatypes::DataType::Boolean,
                false,
            )]),
        ));
        let mut rf = RowFields::empty(std::sync::Arc::clone(&bool_layout));
        rf.set(0, Some(crate::match_engine::Value::Array(vec![])));
        let accs = vec![StatsAccum::Last(Some(std::sync::Arc::new(rf)))];
        assert!(matches!(
            serialize_accs(&accs),
            Err(SpillError::Unsupported(_))
        ));

        // Bool（合法的 others 值）往返不受影响
        let mut rf2 = RowFields::empty(std::sync::Arc::clone(&bool_layout));
        rf2.set(0, Some(crate::match_engine::Value::Bool(true)));
        let accs2 = vec![StatsAccum::Last(Some(std::sync::Arc::new(rf2)))];
        let bytes = serialize_accs(&accs2).expect("serialize");
        let back = deserialize_accs(&bytes, &bool_layout).expect("deserialize");
        let rf_back = back[0].last().as_ref().expect("last");
        assert_eq!(
            rf_back.value_at(0),
            Some(crate::match_engine::Value::Bool(true))
        );
    }

    #[test]
    fn stats_accum_roundtrip_all_variants() {
        let layout = sample_layout();
        // Numeric
        let numeric = StatsAccum::Numeric(Box::new(NumericAccum {
            count: 5,
            sum: 100,
            min: Some(10),
            max: Some(30),
        }));
        // Distinct
        let mut d = DistinctSet::default();
        d.insert(DistinctKey::Int(1));
        d.insert(DistinctKey::Int(2));
        d.insert(DistinctKey::Float(1.5f64.to_bits()));
        d.insert(DistinctKey::Str("x".into()));
        let distinct = StatsAccum::Distinct(Box::new(d));
        // Last
        let mut rf = RowFields::empty(std::sync::Arc::clone(&layout));
        rf.set(0, Some(crate::match_engine::Value::Number(9800.0)));
        rf.set(2, Some(crate::match_engine::Value::Str("Google".into())));
        let last = StatsAccum::Last(Some(std::sync::Arc::new(rf)));
        // Top
        let mut e1 = RowFields::empty(std::sync::Arc::clone(&layout));
        e1.set(1, Some(crate::match_engine::Value::Number(1.0)));
        let top = StatsAccum::Top(vec![TopEntry {
            key: 100.0,
            row: e1,
        }]);

        let accs = vec![numeric, distinct, last, top];
        let bytes = serialize_accs(&accs).expect("serialize");
        let back = deserialize_accs(&bytes, &layout).expect("deserialize");
        assert_eq!(back.len(), accs.len());
        // Numeric 逐字段
        assert_eq!(back[0].numeric().count, 5);
        assert_eq!(back[0].numeric().sum, 100);
        assert_eq!(back[0].numeric().min, Some(10));
        assert_eq!(back[0].numeric().max, Some(30));
        // Distinct 集合
        let StatsAccum::Distinct(d) = &back[1] else {
            panic!("期望 Distinct 变体");
        };
        assert_eq!(d.len(), 4);
        // Last 行字段
        let last_back = back[2].last().as_ref().expect("last");
        assert_eq!(last_back.value_at(0), Some(crate::match_engine::Value::Number(9800.0)));
        assert_eq!(last_back.value_at(2), Some(crate::match_engine::Value::Str("Google".into())));
        // Top
        assert_eq!(back[3].top().len(), 1);
        assert_eq!(back[3].top()[0].key, 100.0);
    }

    #[test]
    fn spill_value_roundtrip_with_layout_mismatch_rejected() {
        let layout = sample_layout();
        let key = ScopeKey::Pair(
            Box::new(ScopeKey::Int(123)),
            Box::new(ScopeKey::Int(456)),
        );
        let accs = vec![StatsAccum::Last(None)];
        let bytes = serialize_spill_value(&key, &accs).expect("serialize");
        let (k, a) = deserialize_spill_value(&bytes, &layout).expect("deserialize");
        assert_eq!(k, key);
        assert_eq!(a.len(), 1);
        assert!(matches!(a[0], StatsAccum::Last(None)));

        // layout 字段数不一致 → Corrupt
        let other_layout = std::sync::Arc::new(RowFieldLayout::from_schema(
            &["only_one".to_string()],
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "only_one",
                arrow::datatypes::DataType::Int64,
                false,
            )]),
        ));
        let mut rf = RowFields::empty(std::sync::Arc::clone(&layout));
        rf.set(0, Some(crate::match_engine::Value::Number(1.0)));
        let accs2 = vec![StatsAccum::Last(Some(std::sync::Arc::new(rf)))];
        let bytes2 = serialize_accs(&accs2).expect("serialize");
        assert!(matches!(
            deserialize_accs(&bytes2, &other_layout),
            Err(SpillError::Corrupt(_))
        ));

        // 尾部残留 → Corrupt
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            deserialize_spill_value(&trailing, &layout),
            Err(SpillError::Corrupt(_))
        ));
    }

    #[test]
    fn noop_spill_is_empty() {
        let mut s = NoopSpillStore;
        assert!(!s.contains(1));
        assert!(s.get(1).is_none());
        assert!(s.drain().is_empty());
        assert_eq!(s.len(), 0);
        assert!(s.put(1, &ScopeKey::Int(1), vec![]).is_ok());
        assert!(!s.contains(1));
    }

    #[test]
    fn mem_spill_roundtrip() {
        let mut s = MemSpillStore::new();
        let key = ScopeKey::Pair(
            Box::new(ScopeKey::Int(1)),
            Box::new(ScopeKey::Int(2)),
        );
        let accs = vec![StatsAccum::Last(None)];
        s.put(spill_hash(&key), &key, accs).expect("put");
        assert!(s.contains(spill_hash(&key)));
        assert_eq!(s.len(), 1);
        let (k, a) = s.get(spill_hash(&key)).expect("get");
        assert_eq!(k, key);
        assert_eq!(a.len(), 1);
        let drained = s.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(s.len(), 0);
    }
}
