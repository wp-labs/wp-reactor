//! 序列化（手写字节编码，非 serde；2026-09-04 自 spill.rs 拆出）。
//!
//! - [`ScopeKey`] 编码与 `scope_key_hash` 的字节序同构（tag + payload），
//!   round-trip 对拍保证与 `comps_match` / `scope_key_from_comps` 一致。
//! - [`StatsAccum`] 按变体 tag 分派；[`RowFields`] 按 layout 槽序写数组
//!   （**layout 不序列化**——读回时按当前 executor 的 layout 解释，同一
//!   executor 生命周期内不变，成立）。
//!
//! ## 正确性红线
//! 反序列化遇损坏数据 → 返回 `Err(SpillError::Corrupt)`（调用方 panic，绝不
//! 静默丢键）。长度字段带上限校验（防恶意/损坏长度导致 OOM）。

use super::*;
use crate::match_engine::ScopeKey;
use crate::match_engine::executor::{
    DistinctKey, DistinctSet, NumericAccum, RowFields, StatsAccum, TopEntry,
};

// ---------------------------------------------------------------------------
// 字节写入器/读取器（小端，长度前缀带上限）
// ---------------------------------------------------------------------------

/// 单键/单桶的序列化长度上限（防护：损坏长度导致 OOM）。ScopeKey 树 8 层、
/// accs 16 度量、行字段 64 字段的合理上界 ~1MB。
const MAX_SERIALIZED_BYTES: usize = 1 << 20;

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SpillStore")]
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

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SpillStore")]
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
pub(crate) const TAG_EMPTY: u8 = 0;
pub(crate) const TAG_INT: u8 = 1;
const TAG_FLOAT: u8 = 2;
const TAG_STR: u8 = 3;
pub(crate) const TAG_PAIR: u8 = 4;

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
pub(crate) const TAG_NUMERIC: u8 = 0;
pub(crate) const TAG_DISTINCT: u8 = 1;
pub(crate) const TAG_LAST: u8 = 2;
pub(crate) const TAG_TOP: u8 = 3;

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
    // 已写 RowFields 的指针表（Last 去重引用, 2026-08-27 q18 落盘 2.9x 压缩）：
    // 同桶多 last 内存共享 1 份 RowFields（row_cache 保证）——序列化只写 1
    // 份完整 + 后续引用索引, 读回共享同一 Arc（与内存语义一致）。
    let mut written_rf: Vec<std::ptr::NonNull<RowFields>> = Vec::new();
    for acc in accs {
        match acc {
            StatsAccum::Numeric(n) => {
                w.u8(TAG_NUMERIC);
                write_numeric_acc(&mut w, n);
            }
            StatsAccum::Distinct(d) => {
                w.u8(TAG_DISTINCT);
                write_distinct_acc(&mut w, d);
            }
            StatsAccum::Last(rf) => {
                w.u8(TAG_LAST);
                write_last_acc(&mut w, rf, &mut written_rf)?;
            }
            StatsAccum::Top(entries) => {
                w.u8(TAG_TOP);
                write_top_acc(&mut w, entries)?;
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

/// Numeric 累加器 payload 编码（tag 已由调用方写入）：count/sum/min/max——
/// sum/min/max 为 i128（可超 i64）——全宽写，读回无截断。
fn write_numeric_acc(w: &mut Writer, n: &NumericAccum) {
    w.u64(n.count);
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

/// Distinct 累加器 payload 编码：ints 集合 + others 集合（集合序非确定性——
/// 读回重建集合, 序无关）。
fn write_distinct_acc(w: &mut Writer, d: &DistinctSet) {
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
        write_distinct_key(w, k);
    }
}

/// Last 行字段 payload 编码（指针表去重: 同桶多 last 共享同一 Arc → 重复引用只
/// 写索引; 读回共享同一 Arc, 与内存语义一致）。`None` = 无行字段。
fn write_last_acc(
    w: &mut Writer,
    rf: &Option<std::sync::Arc<RowFields>>,
    written_rf: &mut Vec<std::ptr::NonNull<RowFields>>,
) -> Result<(), SpillError> {
    match rf {
        Some(rf) => {
            let ptr = std::ptr::NonNull::new(std::sync::Arc::as_ptr(rf) as *mut RowFields)
                .expect("Arc 指针非空");
            if let Some(idx) = written_rf.iter().position(|p| *p == ptr) {
                // 已写过 → 引用索引（复用读回时的同一 Arc）。
                w.u8(2);
                w.u64(idx as u64);
            } else {
                w.u8(1);
                write_row_fields(w, rf)?;
                written_rf.push(ptr);
            }
        }
        None => w.u8(0),
    }
    Ok(())
}

/// Top 条目数组 payload 编码（key + 行字段逐个写）。
fn write_top_acc(w: &mut Writer, entries: &[TopEntry]) -> Result<(), SpillError> {
    w.u64(entries.len() as u64);
    for e in entries {
        w.f64(e.key);
        write_row_fields(w, &e.row)?;
    }
    Ok(())
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
    // 已读 RowFields（Last 去重引用读回——写侧 ptr_eq 去重对应）。
    let mut written_rf: Vec<std::sync::Arc<RowFields>> = Vec::new();
    for _ in 0..n {
        let acc = match r.u8()? {
            TAG_NUMERIC => read_numeric_acc(&mut r)?,
            TAG_DISTINCT => read_distinct_acc(&mut r)?,
            TAG_LAST => read_last_acc(&mut r, layout, &mut written_rf)?,
            TAG_TOP => read_top_acc(&mut r, layout)?,
            other => return Err(SpillError::Corrupt(format!("StatsAccum 未知 tag {other}"))),
        };
        out.push(acc);
    }
    Ok(out)
}

/// Numeric 累加器 payload 解码（tag 已由调用方读取）。
fn read_numeric_acc(r: &mut Reader<'_>) -> Result<StatsAccum, SpillError> {
    let count = r.u64()?;
    let sum = r.i128()?;
    let min = if r.u8()? == 1 { Some(r.i128()?) } else { None };
    let max = if r.u8()? == 1 { Some(r.i128()?) } else { None };
    Ok(StatsAccum::Numeric(Box::new(NumericAccum {
        count,
        sum,
        min,
        max,
    })))
}

/// Distinct 累加器 payload 解码（ints/others 集合; 数量带上限, 防损坏长度 OOM）。
fn read_distinct_acc(r: &mut Reader<'_>) -> Result<StatsAccum, SpillError> {
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
        others.insert(read_distinct_key(r)?);
    }
    Ok(StatsAccum::Distinct(Box::new(DistinctSet::from_parts(
        ints, others,
    ))))
}

/// Last 累加器 payload 解码（flag 0=空 / 1=完整行字段（入表, 供后续引用）/
/// 2=引用索引——越界 → Corrupt）。
fn read_last_acc(
    r: &mut Reader<'_>,
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
    written_rf: &mut Vec<std::sync::Arc<RowFields>>,
) -> Result<StatsAccum, SpillError> {
    let flag = r.u8()?;
    let rf = match flag {
        0 => None,
        1 => {
            let rf = read_row_fields_with_layout(r, layout)?;
            let arc = std::sync::Arc::new(rf);
            written_rf.push(std::sync::Arc::clone(&arc));
            Some(arc)
        }
        2 => {
            // 引用之前已读的 RowFields（共享同一 Arc, 与写侧去重对应）。
            let idx = r.u64()? as usize;
            if idx >= written_rf.len() {
                return Err(SpillError::Corrupt(format!(
                    "RowFields 引用索引 {idx} 越界 ({} 已读)",
                    written_rf.len()
                )));
            }
            Some(std::sync::Arc::clone(&written_rf[idx]))
        }
        other => return Err(SpillError::Corrupt(format!("Last flag 未知 {other}"))),
    };
    Ok(StatsAccum::Last(rf))
}

/// Top 累加器 payload 解码（条目数带上限, 防损坏长度 OOM）。
fn read_top_acc(
    r: &mut Reader<'_>,
    layout: &std::sync::Arc<crate::match_engine::executor::RowFieldLayout>,
) -> Result<StatsAccum, SpillError> {
    let n = r.u64()? as usize;
    if n > MAX_SERIALIZED_BYTES / 64 {
        return Err(SpillError::Corrupt("top 条目超上限".into()));
    }
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let key = r.f64()?;
        let row = read_row_fields_with_layout(r, layout)?;
        entries.push(TopEntry { key, row });
    }
    Ok(StatsAccum::Top(entries))
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
