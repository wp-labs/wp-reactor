//! StatsExecutor spill 集成测试（M3，`docs/design/stats-state-spill-redb.md` §12）。
//!
//! 验证机制（数据驱动）：
//! 1. 超预算 → clock 驱逐最老键到 spill（内存/spill 不相交不变量）
//! 2. 驱逐键再来 → 读回（take）→ 计数继续累积，不丢
//! 3. close → drain + 并入内存 → 每键恰好一次、按 ScopeKey 升序
//! 4. **对拍契约**：spill 与否输出逐值一致（含 last 行字段跨序列化往返）
//! 5. 三层预算阶梯：落盘满 → 回退拒收（不丢内存键）
//! 6. redb store 生命周期：close 后文件删除
//!
//! 本文件收口共享 harness（import / 计划与度量构建 / `exec_with_spill` 等 helper），
//! 测试按主题分派到兄弟子模块（`#[path]` 相对本文件目录，机制同 stats_exec_test.rs；
//! 子模块 `use super::*` 引用本文件项）：
//! - `stats_spill_mem`: Mem store / 无 spill 快速路径——驱逐-读回-close 合并、对拍、
//!   预算阶梯拒收、estimated_bytes 有界、touch/clock、流式 close、distinct/top
//!   度量读回、写失败/盘满预订归还;
//! - `stats_spill_shared`: 规则级共享预算——跨分片 mem/disk 计数、并发驱逐不过度、
//!   惰性创建 × 共享计数、跨窗口归零复用;
//! - `stats_spill_redb`: redb store 全链路与文件生命周期——惰性创建/重建、旧文件
//!   防污染、redb 流式 close、cleanup 幂等。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::Value;
use crate::match_engine::cep::ScopeKey;
use crate::match_engine::executor::stats_exec::StatsExecutor;
use crate::match_engine::spill::{MemSpillStore, RedbSpillStore, SpillStore};
use wf_cep::rows::RowFieldLayout;

// ---------------------------------------------------------------------------
// helpers（与 stats_exec_test 同款）
// ---------------------------------------------------------------------------

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn count_measure(label: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Count,
        field: None,
        arg: None,
    }
}

fn sum_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Sum,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn last_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Last,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn distinct_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::DistinctCount,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn top_measure(label: &str, field: &str, n: u64) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: Some(n),
    }
}

fn keyed_plan(keys: Vec<Expr>, measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn field_key(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.into(), name.into()))
}

fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn extract(row: &HashMap<String, Value>, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

/// 计划的实际桶预算（`bucket_allowance` 口径）——2026-08-27 远程 SoA 重构后
/// count 单度量走 Numeric 载体（264B）, 含 last/top 走 Classic（2.2 校准后
/// 739B）, 单一常量失效。预算语义是「桶数」, 用 allowance 换算。
fn allowance_for(plan: &StatsPlan) -> u64 {
    let soa = plan.measures.iter().all(|m| {
        matches!(
            m.agg,
            StatsAggPlan::Count
                | StatsAggPlan::Sum
                | StatsAggPlan::Avg
                | StatsAggPlan::Min
                | StatsAggPlan::Max
        )
    });
    crate::match_engine::executor::stats_exec::StatsWindowState::bucket_allowance(plan, soa)
}

/// 开启 spill 的 executor（row 路径）：
/// `budget_buckets` = 内存可驻留桶数上限；`store` = 存储实现；
/// `subset` = 行字段子集（None = 无 last/top 度量，`StatsExecutor::new`）。
fn exec_with_spill(
    plan: &StatsPlan,
    budget_buckets: usize,
    subset: Option<Arc<HashSet<String>>>,
    store: Option<Box<dyn crate::match_engine::spill::SpillStore + Send + Sync>>,
    max_spill_bytes: Option<usize>,
) -> StatsExecutor {
    let mut exec = match subset {
        Some(s) => StatsExecutor::with_row_fields(plan.clone(), Some(s)),
        None => StatsExecutor::new(plan.clone()),
    };
    let budget = allowance_for(plan) as usize * budget_buckets;
    exec.set_memory_limit("spill_test", Some(budget));
    exec.set_spill(store, max_spill_bytes, None);
    exec
}

/// 插入 bidder=k 的一行（count+sum(price)）。
fn bid_row(k: i64, price: f64) -> HashMap<String, Value> {
    row(&[("bidder", num(k as f64)), ("price", num(price))])
}

// 测试按主题拆分为兄弟子模块（`#[path]` 相对本文件目录，机制同 stats_exec_test.rs；
// 子模块 `use super::*` 引用本文件收口的共享 harness/import）。
#[path = "stats_spill_mem.rs"]
mod stats_spill_mem;
#[path = "stats_spill_redb.rs"]
mod stats_spill_redb;
#[path = "stats_spill_shared.rs"]
mod stats_spill_shared;
