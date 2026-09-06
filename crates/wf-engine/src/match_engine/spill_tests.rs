//! spill 序列化 / 存储行为测试（2026-09-04 自 spill.rs 内联 `mod tests` 拆出；
//! `#[path]` sibling cfg(test) 子模块）：ScopeKey / StatsAccum 字节编码
//! roundtrip 与损坏/深度/layout 失配拒绝、Noop/Mem/Redb store 行为、redb 文件
//! 生命周期（删旧建新）与流式 drain。

use super::serde::{TAG_DISTINCT, TAG_EMPTY, TAG_INT, TAG_LAST, TAG_NUMERIC, TAG_PAIR, TAG_TOP};
use super::*;
use crate::match_engine::ScopeKey;
use crate::match_engine::executor::{
    DistinctKey, DistinctSet, NumericAccum, RowFieldLayout, RowFields, StatsAccum, TopEntry,
};

fn sample_layout() -> std::sync::Arc<RowFieldLayout> {
    // numeric: price/dateTime；str: channel/url；other: 1 个。
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("dateTime", arrow::datatypes::DataType::Int64, false),
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
    assert_eq!(
        last_back.value_at(0),
        Some(crate::match_engine::Value::Number(9800.0))
    );
    assert_eq!(
        last_back.value_at(2),
        Some(crate::match_engine::Value::Str("Google".into()))
    );
    // Top
    assert_eq!(back[3].top().len(), 1);
    assert_eq!(back[3].top()[0].key, 100.0);
}

/// 同桶多 last 共享同一 Arc<RowFields>（row_cache 保证）→ 序列化只写 1 份,
/// 读回后**仍共享同一 Arc**（ptr_eq）——与内存语义一致（2026-08-27 去重）。
#[test]
fn last_shared_rowfields_dedup_roundtrip_shares_arc() {
    let layout = sample_layout();
    let rf = std::sync::Arc::new({
        let mut r = RowFields::empty(std::sync::Arc::clone(&layout));
        r.set(0, Some(crate::match_engine::Value::Number(7.0)));
        r
    });
    // 3 个 last 指向同一 Arc（同桶多 last 共享的场景）
    let accs = vec![
        StatsAccum::Last(Some(std::sync::Arc::clone(&rf))),
        StatsAccum::Last(Some(std::sync::Arc::clone(&rf))),
        StatsAccum::Last(Some(std::sync::Arc::clone(&rf))),
    ];
    let bytes = serialize_accs(&accs).expect("serialize");
    let back = deserialize_accs(&bytes, &layout).expect("deserialize");
    // 读回共享同一 Arc（去重引用索引生效, 与写侧 ptr_eq 对应）
    let a0 = back[0].last().as_ref().expect("last0");
    let a1 = back[1].last().as_ref().expect("last1");
    let a2 = back[2].last().as_ref().expect("last2");
    assert!(std::sync::Arc::ptr_eq(a0, a1), "读回 last0/last1 共享 Arc");
    assert!(std::sync::Arc::ptr_eq(a0, a2), "读回 last0/last2 共享 Arc");
    assert_eq!(
        a0.value_at(0),
        Some(crate::match_engine::Value::Number(7.0)),
        "去重读回值不丢"
    );
    // 不同 Arc 的 last 不被误合并（引用索引必须精确匹配）
    let rf2 = std::sync::Arc::new({
        let mut r = RowFields::empty(std::sync::Arc::clone(&layout));
        r.set(0, Some(crate::match_engine::Value::Number(8.0)));
        r
    });
    let accs2 = vec![
        StatsAccum::Last(Some(std::sync::Arc::clone(&rf))),
        StatsAccum::Last(Some(std::sync::Arc::clone(&rf2))),
    ];
    let bytes2 = serialize_accs(&accs2).expect("serialize");
    let back2 = deserialize_accs(&bytes2, &layout).expect("deserialize");
    assert!(
        !std::sync::Arc::ptr_eq(
            back2[0].last().as_ref().expect("a"),
            back2[1].last().as_ref().expect("b")
        ),
        "不同内容不共享 Arc"
    );
    assert_eq!(
        back2[1].last().as_ref().expect("b").value_at(0),
        Some(crate::match_engine::Value::Number(8.0))
    );
}

#[test]
fn spill_value_roundtrip_with_layout_mismatch_rejected() {
    let layout = sample_layout();
    let key = ScopeKey::Pair(Box::new(ScopeKey::Int(123)), Box::new(ScopeKey::Int(456)));
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
    assert!(s.take(1).is_none());
    assert!(s.drain().is_empty());
    assert_eq!(s.len(), 0);
    assert!(s.put_batch(vec![(1, ScopeKey::Int(1), vec![])]).is_ok());
    assert!(!s.contains(1));
    s.cleanup();
}

#[test]
fn mem_spill_roundtrip() {
    let mut s = MemSpillStore::new();
    let key = ScopeKey::Pair(Box::new(ScopeKey::Int(1)), Box::new(ScopeKey::Int(2)));
    let accs = vec![StatsAccum::Last(None)];
    s.put_batch(vec![(spill_hash(&key), key.clone(), accs)])
        .expect("put");
    assert!(s.contains(spill_hash(&key)));
    assert_eq!(s.len(), 1);
    // take 只读回（M5-2：不删除——close 由调用方按已读回集合过滤）
    let (k, a) = s.take(spill_hash(&key)).expect("take");
    assert_eq!(k, key);
    assert_eq!(a.len(), 1);
    assert_eq!(s.len(), 1, "take 不删除条目");
    assert!(s.contains(spill_hash(&key)));
    // 覆盖更新后 drain 全部 + 清空
    s.put_batch(vec![(spill_hash(&key), key, vec![StatsAccum::Last(None)])])
        .expect("put");
    let drained = s.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(s.len(), 0);
    s.cleanup();
}

/// 测试用唯一路径（temp 目录 + 名称 + pid + 纳秒，防并行测试撞文件）。
fn spill_test_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "wf_spill_test_{}_{}_{}.rb",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

#[test]
fn redb_spill_roundtrip_and_drain() {
    let layout = sample_layout();
    let path = spill_test_path("redb_roundtrip");
    let mut s = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout)).expect("create");

    let k1 = ScopeKey::Int(1001);
    let accs1 = vec![
        StatsAccum::Numeric(Box::new(NumericAccum {
            count: 2,
            sum: 30,
            min: Some(10),
            max: Some(20),
        })),
        StatsAccum::Last(None),
    ];
    let h1 = spill_hash(&k1);

    let k2 = ScopeKey::Pair(
        Box::new(ScopeKey::Int(1)),
        Box::new(ScopeKey::Str("auction".into())),
    );
    let mut rf = RowFields::empty(std::sync::Arc::clone(&layout));
    rf.set(0, Some(crate::match_engine::Value::Number(9800.0)));
    let accs2 = vec![StatsAccum::Top(vec![TopEntry { key: 1.5, row: rf }])];
    let h2 = spill_hash(&k2);

    s.put_batch(vec![(h1, k1.clone(), accs1), (h2, k2.clone(), accs2)])
        .expect("put_batch");

    assert!(s.contains(h1));
    assert!(s.contains(h2));
    assert!(!s.contains(u64::MAX));
    assert_eq!(s.len(), 2);

    // take 只读回（不删除——redb 旧条目 close 时按已读回集合过滤）
    let (gk, ga) = s.take(h1).expect("take1");
    assert_eq!(gk, k1);
    assert_eq!(ga[0].numeric().count, 2);
    assert_eq!(s.len(), 2, "take 不删除条目");
    assert!(s.contains(h1), "take 后条目仍在");

    // 覆盖更新（读回键再驱逐会 put 覆盖旧条目）后 drain 全部
    s.put_batch(vec![(h1, k1, vec![StatsAccum::Last(None)])])
        .expect("put again");
    let mut drained = s.drain();
    drained.sort_by_key(|(k, _)| format!("{k:?}"));
    assert_eq!(drained.len(), 2);
    // drain 不重写树（M5-2: 紧跟 cleanup 删文件）——条目仍在, cleanup 后消失
    assert_eq!(s.len(), 2, "drain 后条目仍保留（cleanup 删文件）");
    s.cleanup();
    assert!(!path.exists());
}

#[test]
fn redb_reopen_starts_fresh_and_drops_stale_file() {
    // 2026-08-27 review 修正：spill **无持久化语义**（设计 §8）——create 对已
    // 存在文件必须删旧建新（空库），绝不打开旧条目（旧窗键会污染新窗 close
    // drain 的输出）。旧契约「重开 = open 保留数据」与设计相悖，已废弃。
    let layout = sample_layout();
    let path = spill_test_path("redb_reopen");
    let k = ScopeKey::Str("persist".into());
    let h = spill_hash(&k);
    {
        let mut s = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout)).expect("create");
        s.put_batch(vec![(h, k.clone(), vec![StatsAccum::Last(None)])])
            .expect("put");
        // 触发 flush（写队列排空——worker 的 db Arc 释放后文件才可重开）;
        assert!(s.contains(h), "put 后已落盘可见");
    } // Drop 只停写 worker, 不删文件——模拟崩溃残留
    assert!(path.exists(), "残留文件仍在");

    // 重开（create）：删旧建新 → 空库起步, 旧条目不得被打开
    let mut s2 = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout)).expect("reopen");
    assert_eq!(s2.len(), 0, "重开 = 空库（旧条目删除）");
    assert!(!s2.contains(h), "旧键不可见");
    s2.cleanup();
    assert!(!path.exists());
}

#[test]
fn redb_drain_up_to_streams_all() {
    // 流式分批读回（M5-3）：分 3 批读完 5 键, 无重复无遗漏, 尾批后返回空。
    let layout = sample_layout();
    let path = spill_test_path("redb_drain_stream");
    let mut s = RedbSpillStore::create(&path, std::sync::Arc::clone(&layout)).expect("create");
    let mut entries = Vec::new();
    for i in 0..5i64 {
        let k = ScopeKey::Int(1000 + i);
        entries.push((spill_hash(&k), k, vec![StatsAccum::Last(None)]));
    }
    s.put_batch(entries).expect("put_batch");

    let b1 = s.drain_up_to(2);
    let b2 = s.drain_up_to(2);
    let b3 = s.drain_up_to(2);
    let b4 = s.drain_up_to(2);
    assert_eq!(b1.len(), 2);
    assert_eq!(b2.len(), 2);
    assert_eq!(b3.len(), 1, "尾批 1 键");
    assert!(b4.is_empty(), "读完后返回空");
    let mut keys: Vec<i64> = b1
        .into_iter()
        .chain(b2)
        .chain(b3)
        .map(|(k, _)| match k {
            ScopeKey::Int(v) => v,
            _ => panic!("期望 Int"),
        })
        .collect();
    keys.sort();
    assert_eq!(keys, vec![1000, 1001, 1002, 1003, 1004], "全部键恰好一次");
    s.cleanup();
}

/// 反序列化各变体损坏防护（2026-09-06 spill_serde 拆分回归）: 手工构造 accs
/// 字节流, 逐变体触发解码路径的 Corrupt 分支（数量超上限 / 未知 tag / flag 未知 /
/// 引用越界 / 长度超上限 / payload 截断）——锁定 `read_*_acc` 各 helper 的防护
/// 边界（不得因损坏数据 OOM / 索引越界 panic / 静默丢键）。
#[test]
fn deserialize_accs_corrupt_guards_per_variant() {
    let layout = sample_layout();
    let expect_corrupt = |bytes: Vec<u8>| {
        assert!(
            matches!(
                deserialize_accs(&bytes, &layout),
                Err(SpillError::Corrupt(_))
            ),
            "期望 Corrupt, 实际未拒绝: {bytes:?}"
        );
    };
    // accs 数量超上限（> 1024）
    expect_corrupt(1025u64.to_le_bytes().to_vec());
    // 未知 StatsAccum tag
    let mut b = 1u64.to_le_bytes().to_vec();
    b.push(99);
    expect_corrupt(b);
    // Numeric: min 存在但 i128 payload 截断
    let mut b = 1u64.to_le_bytes().to_vec();
    b.push(TAG_NUMERIC);
    b.extend_from_slice(&3u64.to_le_bytes()); // count
    b.extend_from_slice(&0i128.to_le_bytes()); // sum
    b.push(1); // min 有值 → 后续 i128 缺失
    expect_corrupt(b);
    // Distinct: ints 数量超上限
    let mut b = 1u64.to_le_bytes().to_vec();
    b.push(TAG_DISTINCT);
    b.extend_from_slice(&u64::MAX.to_le_bytes()); // n_ints 超上限
    expect_corrupt(b);
    // Last: 未知 flag
    let mut b = 1u64.to_le_bytes().to_vec();
    b.push(TAG_LAST);
    b.push(9);
    expect_corrupt(b);
    // Last: 引用索引越界（尚未读入任何行字段）
    let mut b = 1u64.to_le_bytes().to_vec();
    b.push(TAG_LAST);
    b.push(2); // flag=2 引用
    b.extend_from_slice(&0u64.to_le_bytes()); // idx 0, 无已读行字段
    expect_corrupt(b);
    // Top: 条目数超上限
    let mut b = 1u64.to_le_bytes().to_vec();
    b.push(TAG_TOP);
    b.extend_from_slice(&u64::MAX.to_le_bytes());
    expect_corrupt(b);
}

/// 空值变体 roundtrip（2026-09-06 spill_serde 拆分回归）: Last(None) 的 flag 0、
/// 空 Top / 空 Distinct / 空 Numeric 的 payload 全路径往返不丢信息。
#[test]
fn accs_empty_last_top_distinct_numeric_roundtrip() {
    let layout = sample_layout();
    let accs = vec![
        StatsAccum::Last(None),
        StatsAccum::Top(Vec::new()),
        StatsAccum::Distinct(Box::default()),
        StatsAccum::Numeric(Box::new(NumericAccum {
            count: 0,
            sum: 0,
            min: None,
            max: None,
        })),
    ];
    let bytes = serialize_accs(&accs).expect("serialize");
    let back = deserialize_accs(&bytes, &layout).expect("deserialize");
    assert_eq!(back.len(), 4);
    assert!(back[0].last().is_none(), "Last(None) flag 0 往返");
    assert!(back[1].top().is_empty(), "空 Top 往返");
    let StatsAccum::Distinct(d) = &back[2] else {
        panic!("期望 Distinct 变体");
    };
    assert_eq!(d.len(), 0, "空 Distinct 往返");
    assert_eq!(back[3].numeric().count, 0, "空 Numeric 往返");
    assert_eq!(back[3].numeric().min, None);
    assert_eq!(back[3].numeric().max, None);
}
