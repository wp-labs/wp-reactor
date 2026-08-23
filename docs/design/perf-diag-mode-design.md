# 性能退化定位机制（诊断模式）设计

> 状态：设计定稿（2026-08-23）。方法论见 `docs/PERF_BISECTION_METHOD.md`——本文是
> 把该方法论**内建进引擎**的机制设计：一次实现诊断模式，之后每次性能退化定位 =
> 声明式切换诊断点，**不重启 daemon、不改引擎代码、不手拼测量协议**。

---

## 1. 背景与目标

### 1.1 背景（本机制由一次真实定位驱动）

- `PERF_BISECTION_METHOD.md`（逐段切除定位）在实践中被验证有效：2026-08-23 用它
  定位 qradar "1M 稳态 35k vs nexmark Q1 9.42M" 的差距。
- 但过程中暴露三个机制层面的问题：
  1. **测量假象**：metrics exporter 1s 落盘粒度把 300k floor 钉死在 ~1.06s 下限
     （测得 26万 EPS），修正为 100ms 后真实 **970万**——差 37×，纯测量协议问题；
  2. **逐点起停**：每个诊断点重启 daemon（~1s 启动开销混入测量）+ 手改
     conf/规则文件，流程繁琐、易错；
  3. **完成判定靠轮询**：`append_total + acked_lag` 轮询粒度（100ms）仍是近似信号。

### 1.2 目标

- 性能退化定位成为引擎**内置能力**：诊断模式 + 声明式诊断点，一次实现、长期复用。
- 定位过程**不重启 daemon**：诊断点之间靠热切换（原子门控 + 已有规则 reload）。
- **完成信号内嵌数据流**（漂流瓶）：精确、跨诊断点一致，不再依赖 metrics 轮询粒度。
- 测量协议固化进机制，杜绝测量假象类回归。

### 1.3 非目标 / 设计约束

- **诊断模式只有"禁止"开关**：诊断点全部是"禁止某段以求速/隔离"，不做任何正向
  能力。禁止型开关生产零影响（默认关）。
- **相对增量是目的**：定位要回答"墙在哪一段"，增量成本 = `N/EPS(k) − N/EPS(k−1)`
  的**相对**比较；不做基准绝对数字的精确计量（绝对精度受负载/数据形态影响）。
- 不做 fanout / 窗口 append 切口：concurrency-scaling 实证砍 fanout 会破坏 ack
  链、bench 无法收敛（PERF_BISECTION_METHOD §2.2 第 4 刀）。

---

## 2. 诊断模式总览

```
┌─ wfgen（驱动方）────────────────────────────────────────────┐
│  perf-diag 子命令（编译工具，非脚本）                        │
│  for 诊断点 k in [floor, rules, full, c_*, g_*, ...]:       │
│    ① 切点：POST /admin/v1/perf/point {point=k}              │
│    ② 发帧 batch_k；帧尾追加自描述 marker：                 │
│       {round=k, n=N_k, start_ns=T0}    ← 发送量+开始时间入载荷 │
│    ③ 读 metrics: perf_marker{…, emit_ns}                    │
│    ④ EPS_k = n / (emit_ns − start_ns)  ← 全从 marker 记录算  │
│  输出墙表（每档 EPS + 增量成本 + 墙判定）                    │
└────────────────────────────────────────────────────────────┘
┌─ wfusion daemon（一次启动，不重启）─────────────────────────┐
│  [perf] 诊断模式：禁止型门控（原子，热切）                   │
│    cut_rules / cut_output                                   │
│  runtime.rules hot-reload：规则子集切换（已有能力）          │
│  内置 __perf_marker 窗口 + 标记规则（豁免所有门控，永远活跃）│
│    marker emit 时写 perf_marker{round,n,start_ns,emit_ns}    │
│    四元组齐备 → EPS = n/(emit_ns−start_ns) 直接可算          │
└────────────────────────────────────────────────────────────┘
```

---

## 3. 诊断点与墙梯

### 3.1 诊断点 = 禁止开关组合 + 规则子集

| 诊断点 | cut_rules | cut_output | 规则 | 测的段 |
|---|---|---|---|---|
| `floor` | ✅ 禁止 | ✅ 禁止 | 空 | 注入 + 解码 + 窗口 append |
| `rules` | — | ✅ 禁止 | 全量 | + 规则求值（增量 = 规则墙） |
| `full` | — | — | 全量 | + 输出链（增量 = 输出墙） |
| `family_*` | — | ✅ 禁止 | 按前缀子集 | 家族墙（c_* / g_* / pr_* …） |
| `budget:X` | — | — | 全量 | parse_buffer_bytes=X（**重启例外**） |

- 墙梯**叠加式**（尾部向前切），与 `PERF_BISECTION_METHOD.md` §2 同构。
- 输出墙用 `cut_output` 开关隔离，**不需要动 sinks 拓扑**——`sinks` 是
  RequiresRestart 路径（见 §4.4），用开关绕开。

### 3.2 预算档例外

`parse_buffer_bytes`（preread 预算）在 parse pool 启动时分配 semaphore，**不热切**
（`runtime.*` 均 RequiresRestart，见 wf-config change.rs 分类）。预算档作为唯一
需要重启的诊断点，文档化接受，其余诊断点全部热切。

---

## 4. 引擎改动

### 4.1 `[perf]` 配置段（wf-config）

```toml
[perf]
diag = false        # 诊断模式：启用内置 __perf_marker 窗口（生产默认关）
cut_rules = false   # 禁止规则求值（process_batch 直通，ack 保留）
cut_output = false  # 禁止输出链（emit 不 serialize/stage/commit）
```

- `PerfConfig` 全字段 `#[serde(default)]`，`FusionConfigRaw` / `FusionConfig` 各增
  `perf` 字段。
- `cut_rules` / `cut_output` / `profiling`（复用 `WF_RULE_PROFILING`）均为**原子
  门控**，经 §4.4 端点热切，不进 reload diff（避免污染规则 reload 语义）。

### 4.2 门控切口（wf-runtime）

- **cut_rules**：`RuleTask::process_batch` 入口 early-return——跳过求值/emit，
  **ack 保留**（ack 在 `pull_and_advance` 于 process_batch 返回后执行）→
  append/ack 收敛在 floor 档成立，完成判定可用。
- **cut_output**：`RuleTask::emit` 在 metrics 计数之后、`AlertColumnBuilder::append_record`
  之前 return——跳过 serialize/stage/commit/fanout，**emitted 计数保留**（#18 类
  门禁仍可跑）。
- 门控形态：`set_rule_profiling` 同款全局原子 + `pub fn set_perf_cuts(...)`，
  `Reactor::start` 时从 `config.perf` 初始化。

### 4.3 内置 `__perf_marker` 窗口 + 标记规则（漂流瓶）

- **内置 schema**：`__perf_marker` 流/窗口，字段 `{ round: digit, n: digit,
  start_ns: digit }`（引擎内置，不依赖用户 .wfs；`[perf].diag=true` 时自动注册）。
- **marker 载荷自描述**：wfgen 发送时把**发送量 `n`**（本批事件数）和**开始时间
  `start_ns`**（wfgen 发送开始时钟）写进 marker 事件字段——wfgen 无需外部记账。
- **内置标记规则**：marker 事件 emit 时（复用 emit 路径的 `cached_wall_nanos`
  引擎时钟）写一条完整测量记录：
  `perf_marker{round=k, n=<N_k>, start_ns=<wfgen T0>, emit_ns=<引擎完成时刻>}`。
  该记录四元组齐备，**EPS 直接可算**：`eps = n / (emit_ns − start_ns)`。
- **时钟同一性**：基准同机运行，wfgen 的 `start_ns` 与引擎的 `emit_ns` 同机
  时钟可比（跨机需 NTP 或 marker 由引擎侧回写差值，见 §7）。
- **豁免所有 perf 门控**：cut_rules / cut_output 均不影响 marker 窗口与标记规则——
  保证各诊断点（含 floor 空规则档）都能拿到完成信号。
- **顺序保证**：marker 帧与数据帧同 TCP 连接、同源 seq 有序 → marker 是"批末
  最后一条"，其 emit 时刻 ≈ 该批处理结束时刻（+marker 自身常数量处理开销）。
- **跨档一致性**（决策 ①a）：marker 走独立窗口，测共享段（recv/decode/路由）+
  marker 窗排水；绝对时间可能略早于"最慢数据窗口"的规则消化完，但**增量
  T1(k) − T1(k−1) 不受影响**，墙的归属判定不变。

### 4.4 热切通道（admin API）

- 新增轻量端点 `POST /admin/v1/perf/point {point: "<floor|rules|full|family:...>"}`
  （复用 admin HTTP + bearer token 基础设施）。
- 语义：daemon 侧把端点参数映射为门控组合（cut_rules/cut_output 原子翻转）或
  `runtime.rules` 子集热 reload（已有 HotReloadSupported）。进程内自转，零重启。
- 不复用 `POST /admin/v1/reloads/model`（project-remote 版本化 reload，重机制）。

### 4.5 metrics 协议（防假象，固化）

- `report_interval` **默认 100ms**（当前 nexmark 已用、qradar 曾用 1s 制造假象；
  根治为引擎默认值，改 100ms 后短跑不再被 ~1s 粒度钉死）。
- 完成判定信号 = `perf_marker` 指标（事件驱动），替代 metrics 轮询近似。

---

## 5. wfgen 驱动（`perf-diag` 子命令）

```
wfgen perf-diag \
  --frames <file>                       # 预编码帧（数据部分）
  --addr 127.0.0.1:9800                 # TCP 数据端口
  --admin 127.0.0.1:<admin_port>        # admin API 端口
  --n <N> | --n-list "100k,1m,3m"       # 数据量（单档或递增多档）
  --points "floor,rules,full,c_*,g_*"   # 诊断点列表（默认墙梯）
  --rounds 2                            # 每点轮数（取 max，降负载噪声）
```

- **循环**（每点 k，每轮）：
  1. `POST /admin/v1/perf/point {point=k}` → 轮询 admin status 确认切换完成；
  2. 统计本批发送量 `n_k`（帧行数合计）；`T0 = now()`；`send-arrow` 发数据帧，
     帧尾追加 `__perf_marker{round=k, n=n_k, start_ns=T0}` 帧（同连接、同 seq
     尾部，保证最后处理）；
  3. 轮询 metrics 直到读到 `perf_marker{round=k, …}` 的 `emit_ns`；
  4. `EPS_k = n_k / (emit_ns − start_ns)`——**发送量/开始时间来自 marker 载荷，
     完成时间来自引擎 emit 记录，全程无外部记账**（delta 口径，跨点窗口残留
     状态不影响）。
- **数据由小到大**：`--n-list` 对每个诊断点按递增 N 各测一次——小 N 秒级出方向，
  大 N 确认墙是 per-event 还是固定开销（2026-08-23 的 26万 vs 970万 正是这种
  区分救回来的）。
- **墙表输出**：每档 EPS/CPU%/RSS + 增量成本 + 墙判定（CPU 高且增量大 = 忙墙，
  CPU 低 = 等/供给墙，PERF_BISECTION_METHOD §2.4）。

---

## 6. 定位流程（使用手册）

1. **复现**：`wfgen perf-diag --n-list "1m,3m" --points full` 拿当前全量 EPS；
2. **墙梯**：默认 `--points "floor,rules,full"` —— 规则墙 vs 输出墙在哪；
3. **家族**：`--points "c_*,g_*,pr_*,..."`（规则子集热切）—— 哪类规则退化明显；
4. **预算**：`budget:X` 档（唯一重启例外）—— preread 预算槽位是否墙；
5. **热点**：墙段内用采样/微基准（PERF_BISECTION_METHOD §5）收敛到函数。

---

## 7. 边界与开放项

| 项 | 决策 | 说明 |
|---|---|---|
| marker 队列 | 独立 `__perf_marker` 窗口（①a） | 绝对时间可能略早于最慢数据窗；相对增量不受影响 |
| marker 载荷 | 自描述 `{round, n, start_ns}` | 发送量+开始时间入 marker；引擎补 `emit_ns` → EPS 四元组直接可算 |
| 时钟 | 同机基准，wfgen `start_ns` 与引擎 `emit_ns` 同机时钟 | 跨机需 NTP 或引擎回写差值（未做） |
| sink 热切 | 不做（RequiresRestart） | 输出墙用 cut_output 开关代替，无需动 sinks |
| budget 热切 | 不做（RequiresRestart） | 预算档作为唯一重启例外 |
| marker 处理开销 | 常数量（极小），计入每档 | 增量抵消，不影响墙归属 |
| 窗口残留 | delta 口径，2min 滑窗自动老化 | 跨点不重启可连续跑 |

---

## 8. 落地清单（实现顺序）

1. wf-config：`PerfConfig`（diag/cut_rules/cut_output）+ 解析 + 测试；
2. wf-runtime：`set_perf_cuts` 原子门控 + `process_batch`/`emit` 切口 + 测试；
3. wf-runtime：内置 `__perf_marker` 窗口/规则 + `perf_marker` 指标（`diag=true` 时
   注册）+ 豁免门控测试；
4. wf-runtime：`POST /admin/v1/perf/point` 端点（门控翻转 + 规则子集热切映射）；
5. wf-config：`report_interval` 默认 100ms；
6. wfgen：`perf-diag` 子命令（切点 → 发帧+marker → 读指标 → 墙表）；
7. 文档：`PERF_BISECTION_METHOD.md` 挂接本机制（§6 定位流程即机制用法）。
