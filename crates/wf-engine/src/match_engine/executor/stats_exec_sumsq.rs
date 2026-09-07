//! SumSq（∑v²）度量专项测试（2026-09-07，baseline 基线记录 sum_sq）：
//! - 路由：含 SumSq 的计划落 Classic（SoA 资格不变，见 state.rs 注释）；
//! - 数值：n/sum/sumsq 三元组与手工折叠一致（mean/std 由消费侧推导）；
//! - 路径 parity：行式(process_rows) vs 空键列式域归并 vs 键式列式逐行；
//! - 跨路线 parity：同批 `{count,sum}`（SoA）与 `{count,sum,sumsq}`（Classic）
//!   的 count/sum 输出必须逐值相同——加一列 sumsq 不许改变其它度量。
//! 挂载：stats_exec_test.rs 的 `#[path]` 兄弟子模块（`use super::*` 共享 harness）。

use super::*;

fn sumsq_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::SumSq,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn n_sum_sumsq_plan(where_price_lt_10: bool) -> StatsPlan {
    let w = where_price_lt_10.then(|| price_lt(10.0));
    simple_plan(vec![
        count_measure("n"),
        StatsMeasurePlan {
            label: "s".into(),
            source_alias: "b".into(),
            where_expr: w.clone(),
            agg: StatsAggPlan::Sum,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        },
        StatsMeasurePlan {
            label: "ss".into(),
            source_alias: "b".into(),
            where_expr: w,
            agg: StatsAggPlan::SumSq,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        },
    ])
}

/// 手算基准: prices [3,5,4]（无 where）→ n=3, sum=12, sumsq=50。
#[test]
fn sumsq_plan_routes_classic_and_accumulates() {
    let mut exec = StatsExecutor::new(n_sum_sumsq_plan(false));
    assert!(
        exec.window.soa_layout.is_none(),
        "含 SumSq 的计划必须落 Classic（SoA 资格只认 count/sum/avg/min/max）"
    );
    exec.process_rows(
        &[
            row(&[("price", num(3.0))]),
            row(&[("price", num(5.0))]),
            row(&[("price", num(4.0))]),
        ],
        extract,
    );
    assert_eq!(exec.final_measure_values(), vec![3.0, 12.0, 50.0]);
}

/// 路径 parity（空键 = 列式整批域归并 `accumulate_empty_bucket_classic` 段）:
/// 行式 vs 列式逐值一致（含 where 过滤与缺字段行）。
#[test]
fn sumsq_empty_key_columnar_matches_row_based() {
    let rows = vec![
        row(&[("price", num(3.0))]),
        row(&[("price", num(7.0))]),
        row(&[("price", num(4.0))]),
        row(&[("price", num(11.0))]), // where price<10 → 排除
        row(&[]),                     // 缺字段（列式 = null）→ 排除
    ];
    let batch = rows_to_batch(&rows);

    let mut row_exec = StatsExecutor::new(n_sum_sumsq_plan(true));
    row_exec.process_rows(&rows, extract);

    let mut col_exec = StatsExecutor::new(n_sum_sumsq_plan(true));
    assert!(col_exec.process_batch(&batch), "空键数值计划应可列式化");

    // 通过 where 的数值行 = {3,7,4}: n=4（含 11 与缺字段? 见下注）、s=14、ss=74。
    // 注: count 度量无 where → 计全部 5 行; sum/sumsq 有 where → 只计 <10 的
    // 数值行。域归并与逐行路径必须同口径。
    let (rv, cv) = (
        row_exec.final_measure_values(),
        col_exec.final_measure_values(),
    );
    assert_eq!(rv[0], 5.0, "count 无 where: 全行计数");
    assert_eq!(rv[1], 14.0, "sum where<10: 3+7+4");
    assert_eq!(rv[2], 74.0, "sumsq where<10: 9+49+16");
    assert_eq!(cv, rv, "空键列式（域归并）与行式必须逐值一致");
}

/// 路径 parity（键式 = 列式逐行 `accumulate_column_row`）: group by auction。
#[test]
fn sumsq_keyed_columnar_matches_row_based() {
    let measures = vec![
        count_measure("n"),
        sum_measure("s", "price"),
        sumsq_measure("ss", "price"),
    ];
    let keyed = keyed_plan(vec![field_key("b", "auction")], measures);
    let rows = auction_price_rows(&[(1.0, 3.0), (1.0, 5.0), (1.0, 4.0), (2.0, 7.0), (2.0, 9.0)]);
    let batch = rows_to_batch(&rows);

    let mut row_exec = StatsExecutor::new(keyed.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(keyed);
    assert!(col_exec.process_batch(&batch), "键式数值计划应可列式化");

    let rv = row_exec.final_measure_values_by_bucket();
    let cv = col_exec.final_measure_values_by_bucket();
    // auction=1: [3,12,50]; auction=2: [2,16,130]
    assert_eq!(rv.len(), 2);
    for (r, c) in rv.iter().zip(cv.iter()) {
        assert_eq!(r, c, "键式列式与行式逐桶一致");
    }
    assert!(
        rv.iter()
            .any(|(k, v)| *k == ScopeKey::Int(1) && *v == vec![3.0, 12.0, 50.0])
    );
    assert!(
        rv.iter()
            .any(|(k, v)| *k == ScopeKey::Int(2) && *v == vec![2.0, 16.0, 130.0])
    );
}

/// 跨路线 parity: `{count,sum}`（SoA） vs `{count,sum,sumsq}`（Classic）——
/// 同一批数据下 count/sum 逐值相同（加一列 sumsq 不得改变其它度量）。
#[test]
fn sumsq_classic_preserves_soa_values_of_sibling_measures() {
    let rows = vec![
        row(&[("price", num(3.0))]),
        row(&[("price", num(5.0))]),
        row(&[("price", num(4.0))]),
    ];
    let batch = rows_to_batch(&rows);

    let soa_plan = simple_plan(vec![count_measure("n"), sum_measure("s", "price")]);
    let classic_plan = n_sum_sumsq_plan(false);

    let mut soa = StatsExecutor::new(soa_plan);
    assert!(soa.window.soa_layout.is_some(), "纯 count/sum 走 SoA");
    assert!(soa.process_batch(&batch));

    let mut classic = StatsExecutor::new(classic_plan);
    assert!(classic.window.soa_layout.is_none(), "含 SumSq 落 Classic");
    assert!(classic.process_batch(&batch));

    let soa_v = soa.final_measure_values();
    let classic_v = classic.final_measure_values();
    assert_eq!(classic_v.len(), 3);
    assert_eq!(
        &classic_v[0..2],
        &soa_v[0..2],
        "count/sum 跨路线必须逐值一致（SoA vs Classic）"
    );
    assert_eq!(classic_v[2], 50.0, "sumsq = 3²+5²+4²");
}
