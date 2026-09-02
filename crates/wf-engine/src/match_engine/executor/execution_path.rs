//! 生产执行路径判定（P4 物化单轨化的可观测性基座，§11.3 判据）。
//!
//! `RuleTask::process_batch` 对每批选择执行路径：列式（match 的 P3 FieldView /
//! on-each 列式快路径）还是行式（`batch_to_events` 全量物化）。该判定此前
//! 内联在 wf-runtime 的两个布尔里，测试只能间接断言；本模块把它抽成
//! [`RuleExecutor::execution_path`] 单一事实源：
//!
//! - 生产（rule_task.rs）与断言共用同一函数 → 断言的路径就是执行的路径，
//!   不会因内联逻辑漂移而"断言绿、生产走行式"；
//! - 测试按 §11.2 覆盖清单逐形状断言：已列式形态断言 `DeferredMachine` /
//!   `ColumnarEach`，剩余缺口（8 门控）断言 `EagerRows`——缺口每收一项，
//!   对应断言从 `EagerRows` 翻转为列式变体，单轨化进度可回归。

use crate::match_engine::RuleExecutor;

/// 生产执行路径：单批数据在规则任务里的执行轨。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPath {
    /// match 规则 P3 FieldView 列式喂状态机（`defer_materialize`）。
    /// 前置：raw batch 在、machine 在、DEBUG 关、该窗所有 bind filter 列式。
    /// 不逐行物化 Event（命中行经 `ColumnarEvent` 直读列）。
    DeferredMachine,
    /// on-each 列式快路径（`each_plan_columnar_safe` / `each_pipe_columnar_safe`）。
    /// 前置：raw batch 在、无 machine、无 deferred join、无 key 分片行子集、
    /// DEBUG 关、形状门控过。同样不物化 Event。
    ColumnarEach,
    /// deferred join（`emit at`）规则：驱动事件经列式视图挂起（P4 gap-1 收口
    /// 后，2026-09-02）——免 eager_events 逐行 Event 物化，挂起队列持
    /// `JoinRow::Columnar`（Arc batch + 行号 + 投影）。有 let 绑定的规则在
    /// 挂起时物化一次（`deferred_pending_for` 回退），无 let 全列式。
    /// 前置：raw batch 在、无 machine、deferred join 在、DEBUG 关。
    DeferredPending,
    /// 行式兜底：逐行 `Event` 物化（`batch_to_events[_filtered]`）。
    /// 命中 §11.2 任一缺口或结构前置不满足时落入。这是单轨化的"待收缩面"。
    EagerRows,
}

/// [`RuleExecutor::execution_path`] 的批上下文输入——镜像
/// `RuleTask::process_batch` 运行时查阅的字段（逐项对应，勿增删）。
#[derive(Debug, Clone, Copy)]
pub struct ExecutionPathContext<'a> {
    /// 驱动窗口名（`bind_filters_columnar_safe` 按窗口判定）。
    pub window_name: &'a str,
    /// 本批携带原始 Arrow batch（deferred push / pull 有，relay 带 events 的
    /// push 无）。
    pub raw_batch: bool,
    /// 规则是状态机规则（match/stats/seq，非 on-each）。
    pub machine: bool,
    /// 本批是 key 分片行子集（列式 each 快路径未接行子集）。
    pub shard_rows: bool,
    /// tracing DEBUG 打开（调试详情需要行级 Event 渲染被拒行）。
    pub debug_enabled: bool,
    /// 目标是 sink 直写（each_direct）；否则是中间管道（pipe 门控更严）。
    pub each_direct: bool,
}

impl RuleExecutor {
    /// 本规则在本批上下文下的生产执行路径。
    ///
    /// 与 `RuleTask::process_batch` 的判定逐项同构（该函数是唯一消费方），
    /// 因此测试断言的路径 ≡ 生产实际执行的路径。
    pub fn execution_path(&self, ctx: &ExecutionPathContext<'_>) -> ExecutionPath {
        // 规则是否带 deferred join（`emit at`）：计划的静态属性（RuleTask 的
        // DeferredRuntime 构造同源），不由批上下文传入——矩阵测试据此断言。
        let deferred_join = self.plan.joins.iter().any(|j| j.emit_at.is_some());
        // 与 process_batch 的 defer_materialize 完全一致：列式延迟物化只对
        // 状态机路径安全（非列式 bind filter 在缺失 mask 时全放行会静默丢
        // 过滤子集），且被拒行无 Event 可渲染 DEBUG 引用。
        let defer_materialize = ctx.raw_batch
            && ctx.machine
            && !ctx.debug_enabled
            && self.bind_filters_columnar_safe(ctx.window_name);
        if defer_materialize {
            return ExecutionPath::DeferredMachine;
        }
        // deferred join（`emit at`）驱动列式挂起（P4 gap-1，2026-09-02）：
        // 与 process_batch 的 deferred_columnar gate 同构——无 machine（on-each
        // 才有 deferred join）、有 emit_at join、raw batch 在、DEBUG 关。无
        // let 绑定全列式；有 let 时挂起阶段物化一次（deferred_pending_for 回退）。
        let deferred_pending = !ctx.debug_enabled && !ctx.machine && deferred_join && ctx.raw_batch;
        if deferred_pending {
            return ExecutionPath::DeferredPending;
        }
        // 与 process_batch 的 columnar_each 完全一致：独立于 defer_materialize
        // （后者要求 machine 存在，on-each 无 machine，天然互斥）。
        let columnar_each = !ctx.debug_enabled
            && !ctx.machine
            && !deferred_join
            && !ctx.shard_rows
            && ctx.raw_batch
            && if ctx.each_direct {
                self.each_plan_columnar_safe()
            } else {
                self.each_pipe_columnar_safe()
            };
        if columnar_each {
            return ExecutionPath::ColumnarEach;
        }
        ExecutionPath::EagerRows
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;

    use super::*;
    use wf_lang::ast::{
        BinOp, Bound, BoundVal, CloseMode, Expr, FieldRef, JoinMode, MatchMode, PathSegment,
        ReduceClause, ReduceMeasure, WithinSpec,
    };
    use wf_lang::plan::{
        BindPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, LetPlan, MatchPlan, RulePlan,
        ScorePlan, WindowSpec, YieldField, YieldPlan,
    };

    // -----------------------------------------------------------------------
    // Helpers（本模块自备，`coverage_extra` 的 helper 不可跨模块见）
    // -----------------------------------------------------------------------

    fn empty_match_plan() -> MatchPlan {
        MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            seq: None,
            match_mode: MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        }
    }

    fn simple_rule_plan(
        name: &str,
        match_plan: MatchPlan,
        score_expr: Expr,
        entity_id_expr: Expr,
    ) -> RulePlan {
        RulePlan {
            conv_window: None,
            name: name.to_string(),
            binds: vec![BindPlan {
                alias: "e".to_string(),
                window: "w".to_string(),
                filter: None,
            }],
            lets: Vec::new(),
            match_plan,
            each_plan: None,
            stats_plan: None,
            joins: vec![],
            r#where: None,
            entity_plan: EntityPlan {
                entity_type: "ip".to_string(),
                entity_id_expr,
            },
            yield_plan: YieldPlan {
                target: "alerts".to_string(),
                version: None,
                fields: vec![],
            },
            score_plan: ScorePlan { expr: score_expr },
            pattern_origin: None,
            conv_plan: None,
            limits_plan: None,
        }
    }

    /// on-each 基础规则（q1 形态）：常量 score、entity = 限定 flat 字段、
    /// 无 join/let/where/filter。`each_plan_columnar_safe` 与
    /// `each_pipe_columnar_safe` 均应放行。
    fn each_base() -> RulePlan {
        let mut plan = simple_rule_plan(
            "each_base",
            empty_match_plan(),
            Expr::Number(1.0),
            Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        );
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: None,
        });
        plan
    }

    /// match 基础规则（qradar 形态）：无 bind filter（= 列式安全）。
    fn match_base() -> RulePlan {
        simple_rule_plan(
            "match_base",
            empty_match_plan(),
            Expr::Number(1.0),
            Expr::Field(FieldRef::Simple("sip".into())),
        )
    }

    /// 默认生产上下文：raw batch 在、无 machine、非分片、DEBUG 关、sink 直写。
    fn ctx() -> ExecutionPathContext<'static> {
        ExecutionPathContext {
            window_name: "w",
            raw_batch: true,
            machine: false,
            shard_rows: false,
            debug_enabled: false,
            each_direct: true,
        }
    }

    /// on-each 基础规则 + 单个 emit at deferred join（q4/q8/q9 的驱动形态）。
    fn deferred_each_base() -> RulePlan {
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "bids".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "id".into()),
                right: FieldRef::Qualified("bids".into(), "auction".into()),
            }],
            within: None,
            reduce: None,
            emit_at: Some(Expr::Field(FieldRef::Qualified(
                "e".into(),
                "expires".into(),
            ))),
        }];
        plan
    }

    /// 断言单个形状的路径（带规则名标签，失败时可见）。
    fn assert_path(plan: &RulePlan, expect: ExecutionPath) {
        let name = plan.name.clone();
        let actual = RuleExecutor::new(plan.clone()).execution_path(&ctx());
        assert_eq!(actual, expect, "规则 {name} 生产路径与预期不符");
    }

    // -----------------------------------------------------------------------
    // 生产路径矩阵（§11.2 覆盖清单逐形状断言）
    // -----------------------------------------------------------------------

    #[test]
    fn columnar_shapes_take_columnar_paths() {
        // --- 已列式（§11.2 上表）→ 断言列式路径 ---
        // each 基础（q1 形态）→ on-each 列式快路径。
        assert_path(&each_base(), ExecutionPath::ColumnarEach);

        // each→pipe（q13a 形态，each_direct=false 走 pipe 门控）。
        let mut pctx = ctx();
        pctx.each_direct = false;
        assert_eq!(
            RuleExecutor::new(each_base()).execution_path(&pctx),
            ExecutionPath::ColumnarEach,
            "each→pipe 基础形状必须走 pipe 列式"
        );

        // match 规则（machine 在）→ P3 FieldView 列式喂状态机。
        let mctx = ExecutionPathContext {
            machine: true,
            ..ctx()
        };
        assert_eq!(
            RuleExecutor::new(match_base()).execution_path(&mctx),
            ExecutionPath::DeferredMachine,
            "match 规则必须走 DeferredMachine"
        );

        // each + Snapshot join（q13b/q20 形态：右窗 where 限定引用）。
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "person_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "sip".into()),
                right: FieldRef::Qualified("person_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        plan.r#where = Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified(
                "person_events".into(),
                "id".into(),
            ))),
            right: Box::new(Expr::Number(42.0)),
        });
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // each + let（q22 形态：RHS 列式编译、yield 引用内联）。
        let mut plan = each_base();
        plan.lets = vec![LetPlan {
            name: "x".into(),
            expr: Expr::Number(1.0),
        }];
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // each + 列式 each filter（无 join，Q14 价格区间形态）。
        let mut plan = each_base();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: Some(Expr::Bool(true)),
        });
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // each + 常量×flat score（q1 `0.908 * b.price` 形态）。
        let mut plan = each_base();
        plan.score_plan = ScorePlan {
            expr: Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Number(0.908)),
                right: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            },
        };
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // gap-3（2026-09-02 收口）：each + 后置 where（无 join、可列式）→
        // 列式（批级守卫掩码，行式 where_ok 严格语义对拍锁定）。
        let mut plan = each_base();
        plan.r#where = Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        });
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // gap-3 pipe 变体：each→pipe + 列式 where → pipe 门控同样放行。
        let mut pctx = ctx();
        pctx.each_direct = false;
        let mut plan = each_base();
        plan.yield_plan.target = "pipe_win".into();
        plan.r#where = Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        });
        assert_eq!(
            RuleExecutor::new(plan).execution_path(&pctx),
            ExecutionPath::ColumnarEach,
            "each→pipe + 列式 where 必须走 pipe 列式"
        );

        // gap-4（2026-09-02 收口）：非列式 each filter → 逐行解释回退，仍列式。
        let mut plan = each_base();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: Some(Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            }),
        });
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // gap-4：非列式 bind filter → 列式路径逐行 event_matches_alias 解释。
        let mut plan = each_base();
        plan.binds[0].filter = Some(Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        });
        assert_path(&plan, ExecutionPath::ColumnarEach);

        // gap-5（2026-09-02 收口）：无活 join + list-index 输出字段
        // （`e.tags[0]`，Path=[Field,Index]）→ 编译 ListIndex cvec
        // （Field 快通道只读 flat 列）→ 列式（行式 Path 下标对拍锁定）。
        let mut plan = each_base();
        plan.yield_plan.fields = vec![YieldField {
            name: "tag0".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
            }),
        }];
        assert_path(&plan, ExecutionPath::ColumnarEach);
    }

    #[test]
    fn deferred_gap_shapes_take_deferred_pending_path() {
        // --- §11.2 gap 1/2（q4/q9/q8，deferred join）2026-09-02 已收口：
        // 驱动事件列式挂起（DeferredPending）。reduce/within 的列式执行在
        // evaluate_deferred_join 内（右行 JoinRow::Columnar 读取，早已列式），
        // 剩余缺口是驱动行的物化——已消除。---
        // 1. reduce maxrow + emit at（q4/q9 真实形态）。
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "bids".into(),
            mode: JoinMode::Inner,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "id".into()),
                right: FieldRef::Qualified("bids".into(), "auction".into()),
            }],
            within: None,
            reduce: Some(ReduceClause {
                measure: ReduceMeasure::Maxrow {
                    field: FieldRef::Qualified("bids".into(), "price".into()),
                    tie: None,
                },
                label: None,
            }),
            emit_at: Some(Expr::Field(FieldRef::Qualified(
                "e".into(),
                "expires".into(),
            ))),
        }];
        assert_path(&plan, ExecutionPath::DeferredPending);

        // 2. within/interval + emit at（q8 真实形态）。
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "bids".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "id".into()),
                right: FieldRef::Qualified("bids".into(), "auction".into()),
            }],
            within: Some(WithinSpec {
                lo: Bound {
                    open: false,
                    val: BoundVal::Dur {
                        dur: Duration::from_secs(10),
                        neg: true,
                    },
                },
                hi: Bound {
                    open: false,
                    val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                        "e".into(),
                        "expires".into(),
                    ))),
                },
            }),
            reduce: None,
            emit_at: Some(Expr::Field(FieldRef::Qualified(
                "e".into(),
                "expires".into(),
            ))),
        }];
        assert_path(&plan, ExecutionPath::DeferredPending);
    }

    #[test]
    fn eight_gap_shapes_still_take_row_path() {
        // --- 剩余缺口（§11.2 下表 5-8 项 + 非列式残项）→ 行式 ---
        // 3（残）. each + 后置 where（无 join、**非列式**）→ 仍行式。
        let mut plan = each_base();
        plan.r#where = Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))],
            }),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        });
        assert_path(&plan, ExecutionPath::EagerRows);

        // 4（pipe 变体）. each→pipe + 非列式 filter → pipe 门控仍拒（未接
        // filter 处理）→ 行式。
        let mut pctx = ctx();
        pctx.each_direct = false;
        let mut plan = each_base();
        plan.yield_plan.target = "pipe_win".into();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: Some(Expr::FuncCall {
                qualifier: None,
                name: "upper".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            }),
        });
        assert_eq!(
            RuleExecutor::new(plan).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "each→pipe + 非列式 filter → 行式"
        );

        // 4（活 join 变体）. 列式 each filter + 活 join → 列式 join 富化路径未接
        // filter 求值 → 行式。where 引用右窗让 join 存活。
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "person_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "sip".into()),
                right: FieldRef::Qualified("person_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        plan.r#where = Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified(
                "person_events".into(),
                "id".into(),
            ))),
            right: Box::new(Expr::Number(5.0)),
        });
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: Some(Expr::Bool(true)),
        });
        assert_path(&plan, ExecutionPath::EagerRows);

        // gap-5 负向边界（2026-09-02）：list-index 输出字段 + **活 join** →
        // out_shape 仍拒（无活 join 才放行）→ 行式。
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "person_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "sip".into()),
                right: FieldRef::Qualified("person_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        plan.r#where = Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified(
                "person_events".into(),
                "id".into(),
            ))),
            right: Box::new(Expr::Number(5.0)),
        });
        plan.yield_plan.fields = vec![YieldField {
            name: "tag0".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![PathSegment::Field("tags".into()), PathSegment::Index(0)],
            }),
        }];
        assert_path(&plan, ExecutionPath::EagerRows);

        // gap-5 负向边界：list-index 输出字段 root 引用 **let**（`x[0]`）→
        // 列式无 let 视图（编译 root 缺失 → 全 null 静默失真）→ 行式。
        let mut plan = each_base();
        plan.lets = vec![LetPlan {
            name: "x".into(),
            expr: Expr::Field(FieldRef::Qualified("e".into(), "tags".into())),
        }];
        plan.yield_plan.fields = vec![YieldField {
            name: "tag0".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![PathSegment::Field("x".into()), PathSegment::Index(0)],
            }),
        }];
        assert_path(&plan, ExecutionPath::EagerRows);

        // 5. 输出字段非 flat（有 join 时的歧义裸名）——out_shape 拒绝。
        let mut plan = each_base();
        plan.joins = vec![JoinPlan {
            right_window: "person_events".into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("e".into(), "sip".into()),
                right: FieldRef::Qualified("person_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        plan.yield_plan.fields = vec![YieldField {
            name: "city".into(),
            value: Expr::Field(FieldRef::Simple("city".into())),
        }];
        assert_path(&plan, ExecutionPath::EagerRows);

        // 6. score 非「常量 | 常量×flat」（字段×字段）。
        let mut plan = each_base();
        plan.score_plan = ScorePlan {
            expr: Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "a".into()))),
                right: Box::new(Expr::Field(FieldRef::Simple("b".into()))),
            },
        };
        assert_path(&plan, ExecutionPath::EagerRows);

        // 7. entity 非字面量 / flat 字段（Path 形态）。
        let mut plan = each_base();
        plan.entity_plan.entity_id_expr = Expr::Field(FieldRef::Path {
            alias: "b".into(),
            segments: vec![PathSegment::Field("obj".into())],
        });
        assert_path(&plan, ExecutionPath::EagerRows);

        // 8. yield 非字面量 / flat / 列式输出函数（fmt 模板非字面量）。
        let mut plan = each_base();
        plan.yield_plan.fields = vec![YieldField {
            name: "y".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "fmt".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            },
        }];
        assert_path(&plan, ExecutionPath::EagerRows);
    }

    #[test]
    fn structural_preconditions_force_row_path() {
        // --- 结构前置不满足 → EagerRows ---
        // 无 raw batch（relay push 只带 events）。
        let mut pctx = ctx();
        pctx.raw_batch = false;
        assert_eq!(
            RuleExecutor::new(each_base()).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "无 raw batch → 行式"
        );
        // key 分片行子集。
        let mut pctx = ctx();
        pctx.shard_rows = true;
        assert_eq!(
            RuleExecutor::new(each_base()).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "key 分片行子集 → 行式"
        );
        // DEBUG 开（match 与 each 都回退）。
        let mut pctx = ctx();
        pctx.debug_enabled = true;
        assert_eq!(
            RuleExecutor::new(each_base()).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "DEBUG 开 → 行式（each）"
        );
        let mut pctx = ctx();
        pctx.debug_enabled = true;
        pctx.machine = true;
        assert_eq!(
            RuleExecutor::new(match_base()).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "DEBUG 开 → 行式（match）"
        );
        // deferred join（emit at，q4/q8/q9 结构）→ DeferredPending（P4 gap-1
        // 已收口：驱动列式挂起，非行式）。
        assert_eq!(
            RuleExecutor::new(deferred_each_base()).execution_path(&ctx()),
            ExecutionPath::DeferredPending,
            "deferred join → 列式挂起"
        );
        // deferred join + DEBUG → 行式（被拒行需 Event 渲染调试详情）。
        let mut pctx = ctx();
        pctx.debug_enabled = true;
        assert_eq!(
            RuleExecutor::new(deferred_each_base()).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "deferred join + DEBUG → 行式"
        );
        // deferred join + 无 raw batch（relay push 只带 events）→ 行式。
        let mut pctx = ctx();
        pctx.raw_batch = false;
        assert_eq!(
            RuleExecutor::new(deferred_each_base()).execution_path(&pctx),
            ExecutionPath::EagerRows,
            "deferred join + 无 raw batch → 行式"
        );
        // match + 非列式 bind filter → defer 不成立（机器路径 eager 兜底）。
        let mut plan = match_base();
        plan.binds[0].filter = Some(Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
        });
        let mctx = ExecutionPathContext {
            machine: true,
            ..ctx()
        };
        assert_eq!(
            RuleExecutor::new(plan).execution_path(&mctx),
            ExecutionPath::EagerRows,
            "match + 非列式 bind filter → 行式"
        );
    }
}
