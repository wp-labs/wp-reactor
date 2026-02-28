# WFG 设计（新语法 / 方案 3）

> 本文是 `.wfg` 场景 DSL 的独立设计文档。
> 与 `WFL` 主规范解耦演进；不兼容旧 `.wfg` 语法。

## 1. 目标与边界

- 可读性优先：场景编写者一眼看懂“生成什么、验证什么”。
- 规则验证显式：不允许把规则逻辑藏在语法糖里。
- stream-first：场景只描述 stream 级数据，window 约束由 `.wfs/.wfl` 推导。
- 新系统：不做旧语法兼容。

## 2. 核心约定

- 注释：只支持 `//`。
- `#` 不是注释；`#[]` 用于元信息注解。
- 期望块使用 `expect`（不再使用 `oracle`）。
- 正确性断言语法：
  - `hit(<rule>) >= <percent>`
  - `near_miss(<rule>) <= <percent>`
  - `miss(<rule>) <= <percent>`

## 3. 参考示例

```wfg
use "../schemas/security.wfs"
use "../rules/brute_force.wfl"

#[duration=10m]
scenario brute_force_detect<seed=42> {

  traffic {
    stream auth_events gen 100/s
    stream auth_events gen wave(base=80/s, amp=40/s, period=2m, shape=sine)
  }

  injection {
    hit<30%> auth_events {
      user seq {
        use(login="failed") with(3,2m)
        use(action="port_scan") with(1,1m)
      }
    }

    near_miss<10%> auth_events {
      user seq {
        use(login="failed") with(2,2m)
      }
    }

    miss<60%> auth_events {
      user seq {
        use(login="success") with(1,30s)
      }
    }
  }

  expect {
    hit(brute_force_then_scan) >= 95%
    near_miss(brute_force_then_scan) <= 1%
    miss(brute_force_then_scan) <= 0.1%
  }
}
```

## 4. EBNF（新语法）

```ebnf
scenario_file   = { use_decl } , [ scenario_attrs ] , scenario_decl ;

use_decl        = "use" , STRING ;

scenario_attrs  = "#[" , anno_list , "]" ;

scenario_decl   = "scenario" , IDENT , [ "<" , anno_list , ">" ] , "{" ,
                    traffic_block ,
                    [ injection_block ] ,
                    [ expect_block ] ,
                  "}" ;

anno_list       = anno_item , { "," , anno_item } ;
anno_item       = IDENT , "=" , value ;

traffic_block   = "traffic" , "{" , { stream_stmt } , "}" ;
stream_stmt     = "stream" , IDENT , "gen" , rate_expr ;

rate_expr       = rate_const
                | wave_expr
                | burst_expr
                | timeline_expr ;

rate_const      = NUMBER , "/s" ;
wave_expr       = "wave(" ,
                  "base=" , rate_const , "," ,
                  "amp=" , rate_const , "," ,
                  "period=" , DURATION ,
                  [ "," , "shape=" , ( "sine" | "triangle" | "square" ) ] ,
                  ")" ;

burst_expr      = "burst(" ,
                  "base=" , rate_const , "," ,
                  "peak=" , rate_const , "," ,
                  "every=" , DURATION , "," ,
                  "hold=" , DURATION ,
                  ")" ;

timeline_expr   = "timeline" , "{" , { timeline_seg } , "}" ;
timeline_seg    = DURATION , ".." , DURATION , "=" , rate_const ;

injection_block = "injection" , "{" , { injection_case } , "}" ;
injection_case  = mode_kw , "<" , PERCENT , ">" , IDENT , "{" ,
                    seq_block ,
                  "}" ;
mode_kw         = "hit" | "near_miss" | "miss" ;

seq_block       = IDENT , "seq" , "{" , use_stmt , { use_stmt } , "}" ;
use_stmt        = "use(" , predicate_list , ")" , "with(" , NUMBER , "," , DURATION , ")" ;
predicate_list  = predicate , { "," , predicate } ;
predicate       = IDENT , "=" , literal ;

expect_block    = "expect" , "{" , { expect_stmt } , "}" ;
expect_stmt     = expect_fn , "(" , IDENT , ")" , cmp_op , PERCENT ;
expect_fn       = "hit" | "near_miss" | "miss" ;
cmp_op          = ">=" | "<=" | ">" | "<" | "==" ;

value           = literal | DURATION ;
literal         = STRING | NUMBER | "true" | "false" ;
```

## 5. 语义定义

### 5.1 `#[...]` 场景注解

推荐键（可省略，走默认）：

- `duration`: 场景总时长（建议显式填写）。
- `tick`: 调度粒度，默认 `1s`。
- `rows`: `auto | N`，默认 `auto`。
- `emit`: `deterministic | poisson`，默认 `deterministic`。

语义：

- `rows=auto`：按 `rate × tick` 计算当前 tick 的事件数。
- `rows=N`：每个 tick 固定生成 N 行（覆盖 rate 结果）。
- `emit=deterministic`：每 tick 固定行数。
- `emit=poisson`：以 `rate` 为期望值进行泊松采样。

### 5.2 `traffic`

- `stream <name> gen ...` 只声明 stream 名，不写 window/alias。
- window 与字段约束从 `.wfs/.wfl` 推导。
- `timeline` 段必须连续且不重叠；空洞区间按编译错误处理。

### 5.3 `injection`

- `hit<30%> <stream> { ... }` / `near_miss<10%> ...` / `miss<60%> ...`。
- 同一 `injection` 块中所有占比之和必须 `<= 100%`。
- `<entity> seq { ... }`：按实体键串联序列。
- `use(...) with(count,window)`：
  - `use(...)` 是字段等值条件（必须显式字段名）；
  - `count` 是该步事件个数；
  - `window` 是该步时间窗；
  - 多步默认顺序依赖：后一步发生在前一步完成之后。

### 5.4 `expect`

以“样本标签 + 规则名”计算质量指标：

- `hit(rule)`：`label=hit` 样本中，被 `rule` 检出的比例。
- `near_miss(rule)`：`label=near_miss` 样本中，被 `rule` 误检出的比例。
- `miss(rule)`：`label=miss` 样本中，被 `rule` 误检出的比例。

约束：

- 百分比值域 `0%..100%`。
- 分母为 0（无对应标签样本）时，判定为配置错误并中止。

## 6. 校验规则（最小集）

- `use` 引用文件必须存在且可解析。
- `stream` 名必须在 schema/rule 上下文中可解析。
- 注入标签必须在 `{hit, near_miss, miss}` 中。
- 注入占比必须在 `(0, 100]`。
- `expect` 中引用的规则名必须存在于 `.wfl`。

## 7. 运行闭环

```text
wfg + wfs + wfl
   -> wfgen gen (--send)
   -> wfusion run
   -> actual alerts
   -> wfgen verify
   -> expect 判定 + 报告
```

## 8. 迁移说明

- 旧语法中的 `stream alias from window rate ...`、`inject for ...`、`oracle {...}` 不再作为 新语法 主规范。
- 新语法 仅保留本文件定义语法。

## 9. 扩展规划（建议）

### P1（优先）

- `expect` 扩展：`precision/recall/fpr`、`latency_p95`。
- `seq` 语义扩展：`not(...)` 与 `then/within` 的严格约束定义。
- 实体分布扩展：热点（Zipf）与新老实体比例。

P1 示例（讨论稿）：

```wfg
#[duration=30m]
scenario brute_force_detect<seed=42> {
  traffic {
    stream auth_events gen 200/s
  }

  injection {
    hit<30%> auth_events {
      user seq {
        use(login="failed") with(3,2m)
        then use(action="port_scan") with(1,1m)
      }
    }

    near_miss<10%> auth_events {
      user seq {
        use(login="failed") with(2,2m)
        not(action="port_scan") within(1m)
      }
    }

    miss<60%> auth_events {
      user seq {
        use(login="success") with(1,30s)
      }
    }
  }

  entity_dist auth_events by user {
    zipf(alpha=1.2, hot=20%)
    new_user=5%
  }

  expect {
    hit(brute_force_then_scan) >= 95%
    near_miss(brute_force_then_scan) <= 1%
    miss(brute_force_then_scan) <= 0.1%

    precision(brute_force_then_scan) >= 99%
    recall(brute_force_then_scan) >= 95%
    fpr(brute_force_then_scan) <= 0.5%
    latency_p95(brute_force_then_scan) <= 2s
  }
}
```

### P2（增强真实性）

- 速率模型扩展：`spike`、`jitter`、`diurnal`（昼夜曲线）。
- 跨流注入：同一实体在多 stream 的联动序列。
- 场景矩阵：同一场景的多参数批量运行。

### P3（工程效率）

- 模板化：`template/param` 复用场景片段。
- 基线对比：与历史结果自动比对回归漂移。
- 报告输出：自动生成 markdown/html 对比报告。
