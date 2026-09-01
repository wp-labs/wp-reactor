//! scope key 提取路径微基准（列式直读 vs 行式 extract_key）——2026-08-31
//! 单 key 字符串规则「直读列构 ScopeKey」优化的数据判断依据。
//!
//! 测量对象：
//! - 新路径：`ColumnarEvent::extract_scope_key`（key 字段从 Arrow 列直读构
//!   typed `ScopeKey`，免 `Value`/`Vec` 分配——本次优化）
//! - 旧路径：`extract_key_simple` + `scope_key_from_values`（`Event` 默认 /
//!   优化前每事件每规则执行的工作）
//!
//! 运行（release-only，与 columnar_bench 同款）：
//!   cargo test --release -p wf-engine scope_key_bench -- --ignored --nocapture
//!
//! 数据域对齐 qradar_pk：conn_events 的 sip(Utf8, 10100 源 IP 长尾)/dport(Int64)/
//! blocked(Boolean)，N=1M，与 run.sh 的注入口径一致。
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::FieldRef;

use crate::match_engine::{
    ColumnarEvent, FieldSource, build_field_index, extract_key_simple, scope_key_from_values,
};

/// qradar conn_events 形态批（sip 长尾 10100、dport 1024..65023、blocked 1/7）。
fn qradar_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("dport", DataType::Int64, false),
        Field::new("blocked", DataType::Boolean, false),
    ]));
    let sips: Vec<String> = (0..n)
        .map(|i| format!("10.{}.{}.{}", (i / 65536) % 250, (i / 256) % 256, i % 256))
        .collect();
    let dports: Vec<i64> = (0..n).map(|i| 1024 + (i % 64000) as i64).collect();
    let blocked: Vec<bool> = (0..n).map(|i| i % 7 == 0).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(sips)) as ArrayRef,
            Arc::new(Int64Array::from(dports)) as ArrayRef,
            Arc::new(BooleanArray::from(blocked)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// 一行的事件视图（with_index：生产 deferred 路径形态）。
struct RowCtx {
    batch: RecordBatch,
    index: Arc<crate::match_engine::FieldIndex>,
}

fn row_ctx(n: usize) -> RowCtx {
    let batch = qradar_batch(n);
    let index = build_field_index(&batch);
    RowCtx { batch, index }
}

/// 消化两种路径的结果，防止循环被优化掉。
fn consume(key: Option<crate::match_engine::ScopeKey>, acc: &mut usize) {
    use std::hash::{Hash, Hasher};
    if let Some(k) = key {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        k.hash(&mut h);
        *acc = acc.wrapping_add(h.finish() as usize);
    }
}

/// 单 key Utf8（sip）：列式直读 vs 行式。
#[test]
#[ignore]
fn bench_scope_key_single_utf8() {
    let n = 1_000_000usize;
    let ctx = row_ctx(n);
    let keys = [FieldRef::Simple("sip".into())];

    // 旧路径（Event 默认 / 优化前）：extract_key_simple → scope_key_from_values
    let mut acc = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&ctx.batch, row, Arc::clone(&ctx.index));
        consume(
            extract_key_simple(&ce, &keys).map(|vs| scope_key_from_values(&vs)),
            &mut acc,
        );
    }
    let old_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    std::hint::black_box(acc);

    // 新路径（本次优化）：ColumnarEvent::extract_scope_key 列式直读
    let mut acc2 = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&ctx.batch, row, Arc::clone(&ctx.index));
        consume(ce.extract_scope_key(&keys, None, "c"), &mut acc2);
    }
    let new_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    std::hint::black_box(acc2);

    eprintln!(
        "scope_key 单 key Utf8 (sip):  old={:.1} ns/op   new(列式直读)={:.1} ns/op   delta={:+.1} ns ({:+.1}%)",
        old_ns,
        new_ns,
        new_ns - old_ns,
        (new_ns - old_ns) / old_ns * 100.0
    );
}

/// 单 key Int64（dport）：列式直读 vs 行式。
#[test]
#[ignore]
fn bench_scope_key_single_int() {
    let n = 1_000_000usize;
    let ctx = row_ctx(n);
    let keys = [FieldRef::Simple("dport".into())];

    let mut acc = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&ctx.batch, row, Arc::clone(&ctx.index));
        consume(
            extract_key_simple(&ce, &keys).map(|vs| scope_key_from_values(&vs)),
            &mut acc,
        );
    }
    let old_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    std::hint::black_box(acc);

    let mut acc2 = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&ctx.batch, row, Arc::clone(&ctx.index));
        consume(ce.extract_scope_key(&keys, None, "c"), &mut acc2);
    }
    let new_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    std::hint::black_box(acc2);

    eprintln!(
        "scope_key 单 key Int64 (dport): old={:.1} ns/op   new(列式直读)={:.1} ns/op   delta={:+.1} ns ({:+.1}%)",
        old_ns,
        new_ns,
        new_ns - old_ns,
        (new_ns - old_ns) / old_ns * 100.0
    );
}

/// 复合 key（sip + dport）：Pair 构造成本对比。
#[test]
#[ignore]
fn bench_scope_key_pair() {
    let n = 1_000_000usize;
    let ctx = row_ctx(n);
    let keys = [
        FieldRef::Simple("sip".into()),
        FieldRef::Simple("dport".into()),
    ];

    let mut acc = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&ctx.batch, row, Arc::clone(&ctx.index));
        consume(
            extract_key_simple(&ce, &keys).map(|vs| scope_key_from_values(&vs)),
            &mut acc,
        );
    }
    let old_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    std::hint::black_box(acc);

    let mut acc2 = 0usize;
    let start = Instant::now();
    for row in 0..n {
        let ce = ColumnarEvent::with_index(&ctx.batch, row, Arc::clone(&ctx.index));
        consume(ce.extract_scope_key(&keys, None, "c"), &mut acc2);
    }
    let new_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    std::hint::black_box(acc2);

    eprintln!(
        "scope_key 复合 key (sip+dport):  old={:.1} ns/op   new(列式直读)={:.1} ns/op   delta={:+.1} ns ({:+.1}%)",
        old_ns,
        new_ns,
        new_ns - old_ns,
        (new_ns - old_ns) / old_ns * 100.0
    );
}
