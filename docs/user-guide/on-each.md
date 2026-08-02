# On Each 与逐条打分

## 适用场景

`on each` 适合“输入一条，计算一条，再把结果送给下游继续聚合”的建模方式。

如果你的上游链路已经有 OML 一类的逐条投影/标准化能力，纯事件语义映射通常更建议放到 OML，而不是写成 WFL `on each` 规则。

典型场景：

- 语义事件 enrichment
- 单条风险打分
- 上游先产出 enriched events，下游再做 `match<...>` 聚合

不适合的场景：

- 需要窗口累计后再判断
- 需要 `on close`
- 需要 `close_reason`
- 需要 `count/sum/avg/max` 这类集合级表达式直接参与当前规则判定

## 何时优先放到 OML

下面这类逻辑，优先考虑在 OML 完成：

- 原始字段清洗、补默认值、重命名
- 状态枚举归一化，例如 `status -> risk_score`
- 单条事件的语义标准化，例如 `raw_events -> semantic_events`
- 不依赖窗口、close、时序关系的逐条投影

典型模式是：

- OML：`wparse_events -> semantic_events`
- WFL：`semantic_events -> risk_alerts / entity_stats`

例如下面这类规则：

```wfl
rule event_semantic_project {
    events {
        e : wparse_events && subject != ""
    }

    on each e -> score(
        if lower(e.status) in ("error", "failed") then 90.0 else 40.0
    )

    entity(service, e.subject)

    yield semantic_events (
        event_time = e.event_time,
        subject = e.subject,
        status = e.status,
        risk_score = @score
    )
}
```

如果它只是逐条把原始事件整理成 `semantic_events`，更推荐放到 OML。这样 WFL 可以只保留真正的窗口规则，例如：

- `match<subject:1s:fixed>` 做风险窗口归并
- `match<subject:1m:fixed>` 做实体统计汇总

只有当这层逐条计算明确希望复用 WFL 的 `entity` / `yield` / 下游中间 window 链路能力时，再考虑使用 `on each`。

## 基本语法

```wfl
rule enrich_each_event {
    events {
        e : auth_events
    }

    on each e where e.action == "failed" -> score(70.0)

    entity(ip, e.sip)

    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip,
        username = e.username,
        risk_score = round(@score, 1),
        risk_label = concat("risk=", @score)
    )
}
```

说明：

- `on each <alias>` 中的 `alias` 必须来自同一规则的 `events`
- `on each` 与 `match` 互斥
- `where` 在单条记录上下文中求值
- 当前规则仍然复用 `score` / `entity` / `yield`

## 和 `match` 的区别

`match` 是窗口内按 key 建 instance 的时序匹配。

`on each` 是无状态逐条求值：

- 不需要 key
- 不需要 duration
- 不创建 instance
- 不参与 timeout / flush / eos close

所以这类规则通常更像“流上 map/enrich/score 一次”，而不是“做时序检测”。

## 推荐链式建模

上游规则先做逐条评分：

```wfl
rule enrich_each_event {
    events {
        e : auth_events
    }

    on each e -> score(if e.action == "failed" then 70.0 else 10.0)

    entity(ip, e.sip)

    yield enriched_events (
        event_time = e.event_time,
        sip = e.sip,
        username = e.username
    )
}
```

下游规则再对 enriched 结果做窗口聚合：

```wfl
rule final_risk {
    events {
        x : enriched_events
    }

    match<sip:5m> {
        on event {
            x | count >= 1;
        }
    } -> score(avg(x.__wfu_score) + 10.0)

    entity(ip, x.sip)

    yield final_out (
        sip = x.sip
    )
}
```

这里的关键点是：

- 上游 `on each -> score(...)` 产出的 `__wfu_score` 是“当前规则对单条输入的评分”
- 如果希望把当前规则 score 落成业务字段，可在 `yield` 中直接引用 `@score`，也可以继续参与表达式，例如 `round(@score, 1)`
- 下游看到的是一组 `x` 记录，因此通常要用 `avg/max/sum` 这类聚合来消费 `x.__wfu_score`
- 这些中间系统字段会被编译器自动视为 `enriched_events` 可用字段，不需要在 `.wfs` 里重复声明

不建议直接写：

```wfl
} -> score(x.__wfu_score + 10.0)
```

因为在 `match<...>` 中，`x` 通常表示窗口中的一组记录，不是单值。

## 中间输出字段约定

当 `yield` 目标还要被下游继续消费时，中间 enriched 记录默认只透传必要系统字段：

- `__wfu_score`
- `__wfu_rule_name`
- `__wfu_entity_type`
- `__wfu_entity_id`

这 4 个字段对下游规则是直接可见的；当某个 window 被识别为中间消费目标时，编译器会自动把它们加入可解析字段集合。

默认不透传：

- `__wfu_fired_at`
- `__wfu_emit_time`
- `__wfu_origin`
- `__wfu_summary`

原因是中间层是“可继续计算的数据”，不是“最终告警结果”。

## 时间字段规则

中间 enriched 记录不默认暴露时间类 `__wfu_*` 字段，但下游窗口仍然需要事件时间。

规则是：

- 如果目标 window 定义了 `time = ...`
- 且 `yield (...)` 没有显式给这个 time 字段赋值，runtime 才会自动把当前输入事件时间写入该 time 列
- 这个时间用于下游 `match<...>` 的窗口推进
- 不会额外生成 `__wfu_fired_at` 之类的系统字段

如果你希望时间在下游作为普通字段可见，显式写入 `yield`：

```wfl
yield enriched_events (
    event_time = e.event_time,
    sip = e.sip
)
```

## 当前限制

第一版 `on each` 建议只做单条记录表达式。

应避免在 `on each` 中直接使用：

- `count(...)`
- `sum(...)`
- `avg(...)`
- `min(...)`
- `max(...)`
- `baseline(...)`
- `close_reason`

如果需要集合或窗口语义，把这些逻辑放到下游 `match<...>` 规则。

## 中间依赖约束

中间 window 只能形成单向链路，不能形成环。

允许：

- `auth_events -> enriched_events -> final_out`
- `raw -> stage_a -> stage_b -> alerts`

不允许：

- 规则自己读 `enriched_events` 再写回 `enriched_events`
- 两条规则形成 `a_out -> b_out -> a_out`

这类循环会在编译或启动阶段直接报错。

## 什么时候优先用 `on each`

优先考虑 `on each`，如果你的规则满足下面几点：

- 每条输入都可以独立计算
- 想先把原始事件规范化成语义事件
- 想把单条评分结果沉淀到中间 window
- 想把复杂窗口聚合拆到下游规则

优先考虑 `match`，如果你从一开始就需要：

- 多步序列
- 窗口计数
- 缺失检测
- close 阶段判断
