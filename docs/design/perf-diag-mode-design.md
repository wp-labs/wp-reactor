# 性能退化定位机制（诊断模式）设计

> 状态：设计定稿（2026-08-23）。方法论见 `docs/PERF_BISECTION_METHOD.md`——本文是
> 把该方法论**内建进引擎**的机制设计：一次实现诊断模式，之后每次性能退化定位 =
> 声明式切换诊断档，**不重启 daemon、不改引擎代码、不手拼测量协议**。
>
> 术语：批末完成信号称 **sentinel（哨兵）**——内置 `__wf_sentinel` 窗口 +
> 哨兵任务，载荷 `{round, n, start_ns}` 自描述，处理完成补 `emit_ns` → EPS 四元组
> 直接可算。

---

## 1. 背景与目标

### 1.1 背景（本机制由一次真实定位驱动）

- `PERF_BISECTION_METHOD.md`（逐段切除定位）在实践中被验证有效：2026-08-23 用它
  定位 qradar "1M 稳态 35k vs nexmark Q1 9.42M" 的差距。
- 但过程中暴露三个机制层面的问题：
  1. **测量假象**：metrics exporter 1s 落盘粒度把 300k floor 钉死在 ~1.06s 下限
     （测得 26万 EPS），修正为 100ms 后真实 **970万**——差 37×，纯测量协议问题；
  2. **逐档起停**：每个诊断档重启 daemon（~1s 启动开销混入测量）+ 手改
     conf/规则文件，流程繁琐、易错；
  3. **完成判定靠轮询**：`append_total + acked_lag` 轮询粒度（100ms）仍是近似信号。

### 1.2 目标

- 性能退化定位成为引擎**内置能力**：诊断模式 + 声明式诊断档，一次实现、长期复用。
- 定位过程**不重启 daemon**：诊断档之间靠 sentinel 驱动自切换（原子门控 +
  已有规则 reload）。
- **完成信号内嵌数据流**（漂流瓶 sentinel）：精确、跨诊断档一致，不再依赖
  metrics 轮询粒度；完成信号同时是下一诊断档的切换触发器。
- 测量协议固化进机制，杜绝测量假象类回归。

### 1.3 非目标 / 设计约束

- **诊断模式只有"禁止"开关**：诊断档全部是"禁止某段以求速/隔离"，不做任何正向
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
│  for 诊断档 k in [floor, rules, full, c_*, g_*, ...]:       │
│    ① 若 k>0：轮询 perf_stage{current=k} 确认引擎已切到 k    │
│    ② 发帧 batch_k；帧尾追加自描述 sentinel：               │
│       {round=k, n=N_k, start_ns=T0}    ← 发送量+开始时间入载荷 │
│    ③ 读哨兵文件记录 sentinel{…, emit_ns}                    │
│    ④ EPS_k = n / (emit_ns − start_ns)  ← 全从 sentinel 记录算 │
│  输出墙表（每档 eps/n/rounds；CPU%/增量由脚本算）            │
└────────────────────────────────────────────────────────────┘
┌─ wfusion daemon（一次启动，不重启）─────────────────────────┐
│  进入：--perf-diag conf/perf-diag.toml（启动参数，生产不带） │
│  诊断档状态机（sentinel 驱动自切换，零外部控制面）：         │
│    sentinel(round=k) emit ─▶ 应用档 k+1（门控翻转+规则reload）│
│    ─▶ 写 perf_stage{current=k+1}（切换完成信号）            │
│  内置 __wf_sentinel 窗口 + 哨兵任务（豁免所有门控，活跃）  │
│    emit 写 perf_sentinel{round,n,start_ns,emit_ns}（四元组）  │
│  runtime.rules hot-reload：规则子集切换（已有能力）          │
└────────────────────────────────────────────────────────────┘
```

---

## 3. 诊断档与墙梯

### 3.1 诊断档 = 禁止开关组合 + 规则子集

| 诊断档 | cut_rules | cut_output | cut_append | cut_recv | cut_sink_write | 规则 | 测的段 |
|---|---|---|---|---|---|---|---|
| `recv` | — | — | — | ✅ 禁止 | — | 全量 | 注入 + TCP 接收（非哨兵帧 body 即丢, 2026-08-25 新档） |
| `decode` | — | — | ✅ 禁止 | — | — | 全量 | + 解码（append 前即丢, 2026-08-25 新档） |
| `floor` | ✅ 禁止 | ✅ 禁止 | — | — | — | 空 | + 窗口 append（+ fanout/ack） |
| `rules` | — | ✅ 禁止 | — | — | — | 全量 | + 规则求值（增量 = 规则墙） |
| `emit` | — | — | — | — | ✅ 禁止 | 全量 | + 输出构建（close 列式 + builder + 通道投递; sink 即丢, 2026-08-25 新档） |
| `full` | — | — | — | — | — | 全量 | + 列→行物化 + 序列化 + sink 写（增量 = 序列化 + 写成本） |
| `family_*` | — | ✅ 禁止 | — | — | — | 按前缀子集 | 家族墙（c_* / g_* / pr_* …） |
| `budget:X`※ | — | — | — | — | — | 全量 | parse_buffer_bytes=X（**重启例外**） |

- `recv` 档（cut_recv, 2026-08-25）在 `DataSourceBatchSource` 解码前生效：非哨兵帧只读帧头
  tag（`frame_tag`, 不解 body）即丢——隔离单连接 TCP 字节率（decode 侧 validate_utf8
  ~2.3GB/s 单线程墙）。增量（decode − recv）= 解码成本。
- `decode` 档（cut_append）在 `push_decoded_batch` 入口生效：普通流解码后即丢（不占
  parse 管线槽位、不进窗口/引擎/输出）; **哨兵流豁免**（`__wf_sentinel`）——测量协议
  （sentinel 驱动档位切换 + EPS 计算）必须活着。增量（floor − decode）= 窗口 append
  + fanout 投递成本。
- `emit` 档（cut_sink_write, 2026-08-25）在 `dispatch_batch` 入口生效：AlertBatch 到 sink
  即丢（不物化、不序列化、不写）——隔离「输出构建 + 通道投递」; **哨兵 sink 豁免**（哨兵记录
  经此落盘驱动档位切换）。增量（full − emit）= 列→行物化 + 序列化 + sink 写成本。
  ⚠ 命名：sink 侧门控叫 `cut_sink_write`，与 worker 侧的 `append_*` 指标（record→列
  构建，属输出构建段）区分——旧名 `cut_serialize` 与指标名碰撞易误导，2026-08-26 统一改名。

- ※ **`budget:X` 不是诊断档语法（实现定稿）**：`PerfStage` 无 budget 字段，
  `perf-diag.toml` 里写不出这一档；列在表里只为墙梯口径完整——它实际是人工流程
  （改 `wfusion.toml` 的 `parse_buffer_bytes` + 重启 daemon，见 §3.2）。
- 墙梯**叠加式**（尾部向前切），与 `PERF_BISECTION_METHOD.md` §2 同构。
- 输出墙用 `cut_output` 开关隔离，**不需要动 sinks 拓扑**——`sinks` 是
  RequiresRestart 路径（见 §4.4），用开关绕开。

### 3.2 预算档例外

`parse_buffer_bytes`（preread 预算）在 parse pool 启动时分配 semaphore，**不热切**
（`runtime.*` 均 RequiresRestart，见 wf-config change.rs 分类）。预算档作为唯一
需要重启的诊断档，文档化接受，其余诊断档全部热切。

**形态（实现定稿）**：预算档**没有对应的 TOML 字段**（`PerfStage` 只有 `name /
cut_rules / cut_output / rules`），因此不是诊断档语法，而是人工流程：改
`wfusion.toml` 的 `parse_buffer_bytes` → 重启 daemon → 重跑墙梯，逐个预算值对比。

---

## 4. 引擎改动

### 4.1 诊断模式进入与配置（启动参数 + 独立文件）

诊断模式由**启动参数**进入，生产启动不带参数即完全关闭（`wfusion.toml` 零污染）：

```
wfusion daemon --perf-diag conf/perf-diag.toml
```

`conf/perf-diag.toml`（独立配置文件，bench 各自一份）——**入口是 `--perf-diag`
参数本身，文件只承载诊断档列表**（顶层 `diag`/`cut_rules`/`cut_output` 是历史
遗留，已被移除）：

```toml
# 诊断档列表（sentinel 驱动依次应用；缺省/空 = 无切换，仅初始门控全 false）
[[stages]]
name = "floor"
cut_rules = true
cut_output = true
rules = ""           # 空 = 保持当前规则；否则规则文件路径（触发热 reload）

[[stages]]
name = "rules"
cut_rules = false
cut_output = true
```

- `PerfConfig`（wf-config）：仅 `stages: Vec<PerfStage{name, cut_rules,
  cut_output, rules}>`，全字段 `#[serde(default)]`；由 `--perf-diag` 参数加载，
  **不进 `wfusion.toml`**。
- `cut_rules` / `cut_output` 为**原子门控**，由诊断档状态机（§4.4）翻转，不进
  reload diff（`PerfStage` 也只有这两个门控字段）。
- **profiling 不是诊断档门控（实现定稿）**：规则相位计时是**独立的启动期环境
  变量门控** `WF_RULE_PROFILING=0`——`Reactor::start` 启动时读一次并
  `set_rule_profiling`（`lifecycle/mod.rs:293`），**诊断档状态机不翻转它**，也不进
  `perf-diag.toml`。诊断时需要关掉计时开销（规则热路径实测 ~7.6% 活跃 CPU），
  就在启动 daemon 时带上该环境变量——全程生效，各档口径一致。
- **初始门控语义（实现定稿）**：启动即应用 `stages[0]` 的门控并写
  `stage{current=0}`——第一档（floor）不依赖任何哨兵即可测得；无诊断档 →
  初始门控全 false（哨兵窗口仍注册，无切换）。

### 4.2 门控切口（wf-runtime）

- **cut_rules**：`RuleTask::process_batch` 入口 early-return——跳过求值/emit，
  **ack 保留**（ack 在 `pull_and_advance` 于 process_batch 返回后执行）→
  append/ack 收敛在 floor 档成立，完成判定可用。
- **cut_output**：`RuleTask::emit` 在 metrics 计数之后、`AlertColumnBuilder::append_record`
  之前 return——跳过 record→列构建/通道/sink 物化+序列化+写，**emitted 计数保留**（#18 类
  门禁仍可跑）。`emit` / `emit_batch` 均如此（计数在门控之前）。
  - **on-each 直投路径例外（实现定稿）**：`emit_each_direct` /
    `emit_each_direct_batch` / `emit_each_direct_batch_columnar` /
    `emit_each_direct_batch_columnar_join` 四个直投函数的门控 return 在计数
    **之前**——该路径的 emitted 计数与 append 耦合，切了 append 就**留不住计数**
    （实测：nexmark q1 `on each` 在 `rules` 档的 `emitted_total` 为 0；4 档墙梯
    warmup+floor+rules+full 的总 emitted = 2×9.2M 而非 3×9.2M）。
  - **影响墙增量的解释口径**：on-each 类规则的 `rules → full` 增量**包含
    `OutputRecord` 构造成本**（构造在门控之后）；而 match 类规则的构造成本已计入
    `rules` 档——两类规则的「输出墙」不同口径，不可直接横向比。
- 门控形态：`set_rule_profiling` 同款全局原子 + `pub fn set_perf_cuts(...)`，
  `Reactor::start` 时从 `--perf-diag` 加载的 `PerfConfig` 初始化（无参数 = 全关）。
  （`set_rule_profiling` 仅作形态参照，它本身不由诊断档翻转，见 §4.1。）

### 4.3 内置 `__wf_sentinel` 窗口 + 哨兵任务（漂流瓶）

- **内置 schema**：`__wf_sentinel` 流/窗口，字段 `{ round: digit, n: digit,
  start_ns: digit }`（引擎内置，不依赖用户 .wfs；`--perf-diag` 启用时自动注册）。
- **sentinel 载荷自描述**：wfgen 发送时把**发送量 `n`**（本批事件数）和**开始时间
  `start_ns`**（wfgen 发送开始时钟）写进 sentinel 事件字段——wfgen 无需外部记账。
- **内置哨兵任务**：哨兵任务在**数据窗排空后**取 `wall_nanos()`
  （`perf_diag.rs:223`，`SystemTime::now()` 纪元纳秒，与 wfgen 的 `start_ns` 同一
  时钟域；取值点在 `process_sentinel_push`，`wait_for_data_drain` 之后）——
  **不复用** rule_task 的批级缓存时钟 `cached_wall_nanos`（批入口缓存一次，排空
  等待期间已陈旧）。据此写一条完整测量记录：
  `perf_sentinel{round=k, n=<N_k>, start_ns=<wfgen T0>, emit_ns=<引擎完成时刻>}`。
  该记录四元组齐备，**EPS 直接可算**：`eps = n / (emit_ns − start_ns)`。
- **记录输出（文件 sink，case 配置）**：sentinel 告警走既有 alert 链，由 case 的
  `topology/sinks/business.d/sentinel.toml` 落盘 `data/perf_sentinel.ndjson`
  （JSONL，一行一条记录）——**wfgen 从该文件读记录**，比从 metrics 流解析干净。
  ⚠ sink 路由组必须放 **`business.d/`**：`infra.d` 只读 `default/error/monitor`
  三个固定文件（`load_infra_group`），`[sink_group]` 形状在 business.d 才是
  按窗口模式匹配的路由组。**该文件由 `wfadm init` 自动生成**（`wfadm` 模板
  `topology/sinks/business.d/sentinel.toml`），新 bench 无需手写。
  **独立验证 case**：`wf-examples/performance/perf_diag_case/`（单流、21 规则、
  100k 事件）承载机制端到端验证（验收清单见其 README/verify.sh），不与
  nexmark_pk/qradar_pk 基准混在一起；真实 bench 需要时各自补同款 sink。
- **记录路径豁免 cut_output**：完成信号不能随输出墙被切——哨兵记录（告警落盘）
  由独立哨兵任务处理，天然不受 `cut_rules`/`cut_output` 门控影响（与数据规则
  区分，见 §4.2）。单批仅 1-2 条，常数量处理开销，增量抵消。

  ```toml
  # topology/sinks/business.d/sentinel.toml（各 bench 一份）
  [sink_group]
  name = "sentinel_infra"
  windows = ["__wf_sentinel"]
  [[sink_group.sinks]]
  connect = "file_json_sink"
  name = "sentinel_out"
  [sink_group.sinks.params]
  base = "data"
  file = "perf_sentinel.ndjson"
  ```
- **时钟同一性**：基准同机运行，wfgen 的 `start_ns` 与引擎的 `emit_ns` 同机
  时钟可比（跨机需 NTP 或由引擎侧回写差值，见 §7）。
- **豁免所有 perf 门控**：cut_rules / cut_output 均不影响 sentinel 窗口与哨兵
  处理——保证各诊断档（含 floor 空规则档）都能拿到完成信号。
- **精度（实现定稿）**：`start_ns`/`emit_ns` 为 epoch nanos（≈1.7e18），超出
  f64 精确范围（ulp ≈ 256ns）。哨兵记录的这两个字段以**字符串**携带
  （`Value::Str` + Chars 类型，JSON `"1727…"`），wfgen 解析回 i64 精确计算
  EPS；`round`/`n` 走 Digit（JSON 整数）。哨兵帧（wfgen→引擎）本身是 Int64
  Arrow 列，全程无精度损失。
- **批末语义（实现定稿）**：哨兵帧与数据帧同 TCP 连接、同源 seq 有序（哨兵是
  "批末最后一条"；`send-arrow/stream --sentinel` 走**分连接哨兵**，见下条）。
  规则消费是异步 pull——哨兵任务在**数据窗排空**后才写记录，`emit_ns` ≈ 该批
  真实处理结束时刻。无此等待时小批量会把"提交完成"当成"处理完成"（100k 实测
  full 档 10.9M 假象）。
  **哨兵帧路由**：TCP source 按 stream tag（`__wf_sentinel`）路由进内置哨兵
  窗口（无时间列，不推水位/不拒迟到），窗口 fanout → 哨兵任务订阅通道
  （Reactor 在 receiver 注册前 spawn 订阅，杜绝首帧漏投）→ 哨兵任务
  `run_sentinel_task` 收到 `RulePush` 后先排空、再补 `emit_ns` 落盘。
  **排空判定（`wait_for_data_drain`）**：遍历 registry 每个数据窗，检查
  `min_acked ≥ next_seq`（哨兵窗自身、无消费语义的 provider 窗跳过）：
  - `next_seq` = 窗口已 append 的批次数（`Window::next_seq`，append 时
    `fetch_add(1)`）——"数据已全部进入窗口"；
  - `min_acked` = **所有**规则消费者（含分片 shard）已确认读到的最小 seq
    （`WindowProgress`，规则处理完批次后 ack）——"最慢的规则也已消费到最新"；
  两者相等 = 该窗"接收完 **且** 全部被规则消费完"。1ms 轮询直到全部排空；
  引擎关停经 cancel 令牌退出（bench 侧另有 MAX_SEC 超时 + metrics-append 兑底）。
  `min_acked` 复用引擎既有消费进度记账——内存驱逐器（evictor）同样以它做
  下限（时间驱逐只删所有消费者已读过的批次，慢规则未读的不能丢），哨兵排空
  不新增机制。
- **多连接注入（分连接哨兵，round=连接号）**：`wfgen perf-diag` 是**单连接**发送
  （`send_payload` 只开一条 TCP，哨兵在帧尾同连接追加，见 §5）。而 bench 侧精确
  EPS 统计用的 `wfgen send-arrow --sentinel N --connections M` 走**分连接哨兵**
  （`cmd_frames.rs:173-177, 209-213`）：
  - 每条连接 copy 完自己的数据后**在自身尾部**追加哨兵帧
    `{round=连接号, n=该连接实际行数, start_ns=该连接开始}`——单连接 = 1 条
    （round=0，兼容旧语义）；多连接 = M 条（round=0..M-1）；
  - 三种注入形态都走同一协议：多连接各推整文件（raw）、预分片逐文件纯 copy
    （`--shard-files`）、键闭包动态分片（`--shard-keys`）；
  - `wfgen stream --sentinel N`（实时流）是**单连接预算模式**：发满 N 条后末尾
    追加哨兵帧 `{round=0, n=total, start_ns}`（`cmd_stream.rs:256-263`）；
  - 引擎哨兵任务等**全部数据窗排空**后为每条连接写四元组——bench 侧聚合：
    `EPS = Σn / (max emit_ns − min start_ns)`（`Σn` = 该批总行数，`min start` =
    最先开始的连接，`max emit` = 最后完成的连接；`sentinel_tuple` 读全部记录）；
  - 各连接 `dt = emit−start` 可单独对比——**连接均衡/慢连接诊断**（某连接 dt
    显著偏大 = 该连接供给不足或分片不均，如 shard 桶阻塞）；
  - **round 字段过载**：perf-diag 场景 round=档索引 k（驱动状态机切换）；
    send-arrow/stream 场景 round=连接号。两者走同一哨兵窗口与 `on_sentinel(round)`
    ——send-arrow 场景 `stages.get(round+1)` 越界即 no-op（`perf_diag.rs:306`），
    **bench 用无档配置（空 stages）跑精确 EPS**：门控全 false、无切换、哨兵窗口
    仅作完成信号，round=连接号不产生任何档位副作用。
- **跨档一致性**：哨兵走独立窗口，测共享段（recv/decode/路由）+ 数据窗排空；
  各档同一口径，增量 T1(k) − T1(k−1) 的墙归属判定成立。

### 4.4 诊断档状态机（sentinel 驱动自切换，无外部控制面）

- **切换触发器 = sentinel emit**：哨兵任务处理 sentinel(round=k) 时向诊断控制器
  投递"档 k 完成"，控制器同步应用档 k+1：
  1. 原子门控翻转（`cut_rules` / `cut_output`）；
  2. 规则子集变化（`stages[k+1].rules` 非空且不同于基线）→ 触发已有
     `runtime.rules` 热 reload（HotReloadSupported；经 Reactor control_handle，
     Applied/Blocked 均算切换完成）；
  3. 返回后哨兵任务写 `stage{current=k+1}` 完成信号（与 sentinel 记录同批、
     同 sink 通道——文件内顺序 = 切换信号先于哨兵记录）。
- **完成信号形态（实现定稿）**：`stage{current=k}` 与 `sentinel{round,n,
  start_ns,emit_ns}` 均写进 `perf_sentinel.ndjson`（JSONL，`record_type` 区分），
  wfgen 读文件即得——不新增 metrics 流解析（`perf_sentinel`/`perf_stage` 独立
  指标不再写，文件记录即跨档一致性校验的单一事实源）。
- **同步性**：切换与 sentinel 处理同路径完成（reload 经 control_handle 等待其
  完成后才返回）；wfgen 读到 `sentinel{k}` 记录时档 k+1 已生效——无竞态。
- **在途批次不受影响**：门控在 `process_batch` 入口检查，round k 的在途尾巴在旧
  门控下自然跑完；round k+1 新数据吃新门控（delta 口径，跨档不串扰）。
- **零新增控制面**：无 admin 端点 / HTTP / 外部切换指令——只有"发数据"与"读
  记录文件"。不复用 `POST /admin/v1/reloads/model`（project-remote 版本化 reload，
  重机制；规则子集切换走既有 `runtime.rules` 热 reload 通道）。

### 4.5 metrics 协议（防假象，固化）

- `report_interval` **默认 100ms**（当前 nexmark 已用、qradar 曾用 1s 制造假象；
  根治为引擎默认值，改 100ms 后短跑不再被 ~1s 粒度钉死）。
- 完成判定信号 = 哨兵文件记录（事件驱动，`stage{current=k}`），替代 metrics
  轮询近似。

---

## 5. wfgen 驱动（`perf-diag` 子命令）

```
wfgen perf-diag \
  --frames <file>                       # 预编码帧（数据部分；覆盖 max(--n-list) 行）
  --diag conf/perf-diag.toml            # 诊断配置（与 daemon 同一份；档列表=轮数）
  --addr 127.0.0.1:9800                 # TCP 数据端口
  --n-list "100k,1m,3m"                 # 数据量（递增多档；缺省 = 帧文件全部行）
  --rounds 1                            # 每档轮数（实现定稿：仅 1 有效，见下）
  --sentinels data/perf_sentinel.ndjson # 哨兵记录文件（缺省同上）
  --output data/perf_diag_wall.txt      # 墙表输出（缺省同上）
  --timeout-secs 90                     # 单次等待超时（clap 默认 60）
```

- 诊断档列表的唯一来源是 `--diag` 指向的 `perf-diag.toml`（daemon 与 wfgen 读同一
  份）；wfgen 按档数发送轮次，不另设 `--stages`。
- **`--rounds` 语义（实现定稿）**：sentinel 驱动的切换在首个哨兵后即发生，同档
  的重复轮次会吃到**下一档**的门控——每档只测一轮（`rounds=1`）；去噪用
  `--n-list` 递增 N（小 N 秒级出方向，大 N 确认墙是 per-event 还是固定开销）
  ——**仅限单档配置**，见下条。
- **`--n-list` 语义（实现定稿）**：循环嵌套是
  `for stage k { for n_target in n_list { for r in rounds } }`
  （`cmd_perf_diag.rs:409`）。引擎侧 `on_sentinel(round=k)` 在**首个**哨兵后就把门控
  翻到 k+1，且 `target <= current` 只保证重复幂等、**不回滚**（`perf_diag.rs:300`）
  ——所以同一档的第 2 个 N 与第 2 轮同病：都吃下一档的门控。结论：「去噪用
  `--n-list` 递增 N」**只在单档配置（`stages` 仅 1 个）下成立**；**多档墙梯下
  `--n-list` 必须只给一个值**，多个 N 要在外层循环里每个 N 重跑（重启）一套完整
  墙梯。

- **循环**（每档 k）：
  1. 轮询哨兵文件直到 `stage{current=k}`——引擎已完成档 k 的切换（多点模式
     启动即 stages[0] + 初始信号；k>0 由哨兵驱动），随后发送无竞态；
  2. 取覆盖 `n_k` 行的帧前缀（帧行数合计 ≥ n_k，`n_k` 计入哨兵载荷）；
     `T0 = now()`；发数据帧 + 帧尾追加
     `__wf_sentinel{round=k, n=n_k, start_ns=T0}` 帧（同连接、同 seq 尾部，
     保证最后处理）；
  3. 从 `data/perf_sentinel.ndjson` 读到 `sentinel{round=k, n=n_k}` 的第
     r 条记录（含引擎补的 `emit_ns`）——引擎侧由哨兵任务落盘；
  4. `EPS_k = n_k / (emit_ns − start_ns)`——**发送量/开始时间来自 sentinel 载荷，
     完成时间来自引擎记录，全程无外部记账**（delta 口径，跨点窗口残留
     状态不影响）。
- **数据由小到大**：`--n-list` 对每个诊断档按递增 N 各测一次——小 N 秒级出方向，
  大 N 确认墙是 per-event 还是固定开销（2026-08-23 的 26万 vs 970万 正是这种
  区分救回来的）。**单档配置口径**；多档墙梯见上面的 `--n-list` 实现定稿。
- **墙表输出（实现定稿）**：墙表只写**最小事实源**——每行
  `<档名>  eps=<EPS> n=<N> rounds=<轮数>`（`cmd_perf_diag.rs:447`），无 CPU%/RSS、
  无增量成本、无墙判定列（与 §4.4「文件记录即单一事实源」同一原则，驱动侧不
  重复记账）。CPU%/RSS 与增量成本、墙判定（CPU 高且增量大 = 忙墙，CPU 低 =
  等/供给墙，PERF_BISECTION_METHOD §2.4）**由 bench 侧脚本消费哨兵记录自行计算**：
  哨兵四元组的 `[start_ns, emit_ns]` 区间可与带 epoch 纳秒时间戳的采样对齐切分，
  从而给出每档 CPU%/RSS。参考实现：`wf-examples/performance/nexmark_pk/diag.sh`。

---

## 6. 定位流程（使用手册）

1. **复现**：`perf-diag.toml` 只含 `full` 一个档，`wfgen perf-diag --n-list "1m,3m"`
   拿当前全量 EPS；
2. **墙梯**：`perf-diag.toml` 配 `floor → rules → full` 三档 —— 规则墙 vs 输出墙
   在哪；
3. **家族**：`stages` 配 `c_*,g_*,pr_*,...` 子集档（规则热切）—— 哪类规则退化明显；
4. **预算**：`budget:X` 档（唯一重启例外）—— preread 预算槽位是否墙；
5. **热点**：墙段内用采样/微基准（PERF_BISECTION_METHOD §5）收敛到函数。

---

## 7. 边界与开放项

| 项 | 决策 | 说明 |
|---|---|---|
| sentinel 队列 | 独立 `__wf_sentinel` 窗口（①a） | 哨兵任务等**数据窗排空**（min_acked 追平 next_seq）后写记录；跨档同一口径 |
| sentinel 载荷 | 自描述 `{round, n, start_ns}` | 发送量+开始时间入 sentinel；引擎补 `emit_ns` → EPS 四元组直接可算；start/emit 以字符串携带防 f64 丢精度 |
| 多连接注入 | 分连接哨兵（round=连接号） | `send-arrow --sentinel --connections M`：每连接尾部各自追加哨兵帧，引擎等全部数据窗排空后逐连接写四元组；EPS = Σn/(max emit − min start)，各连接 dt 可对比做均衡诊断；无档配置下 round=连接号无档位副作用（见 §4.3） |
| 切换机制 | sentinel 驱动自切换 | 同步无竞态、零控制面；在途批次不受影响 |
| 时钟 | 同机基准，wfgen `start_ns` 与引擎 `emit_ns` 同机时钟 | 跨机需 NTP 或引擎回写差值（未做） |
| sink 热切 | 不做（RequiresRestart） | 输出墙用 cut_output 开关代替，无需动 sinks |
| budget 热切 | 不做（RequiresRestart） | 预算档作为唯一重启例外 |
| sentinel 处理开销 | 常数量（极小），计入每档 | 增量抵消，不影响墙归属 |
| 窗口残留 | delta 口径，2min 滑窗自动老化 | 跨档不重启可连续跑 |
| rounds / n-list | 每档仅**首个哨兵**有效 | 首个哨兵即切下一档（`target <= current` 只幂等、不回滚）；故同档的第 2 轮与第 2 个 N 都吃下一档门控——`--rounds` 保留但恒取 1；`--n-list` 多值仅单档配置可用，多档墙梯每个 N 各重跑一套墙梯 |
| 非诊断模式哨兵帧 | 未知流 window miss | 未注册 `__wf_sentinel` 窗口 → `subscribers_of` 为空 → WARN 一次（按 source+tag 去重）+ miss 计数 + 丢弃；数据帧不受影响，门控全 false 无切换——`wfgen perf-diag` 对非诊断 daemon 会**超时报错**（错误信息含根因提示）而非给出错误数字。daemon 启动日志明示 perf-diag 启用状态 |

---

## 8. 质量门禁（实现验收）

1. **新增代码单测覆盖率 ≥ 90%**（`cargo llvm-cov` 行覆盖率口径）：
   - wf-config `PerfConfig` 解析（含 stages 列表、缺省值）；
   - wf-runtime 门控切口（cut_rules 直通保留 ack / cut_output 保留 emitted 计数，
     on-each 直投路径按 §4.2 例外不保留计数）；
   - 内置 `__wf_sentinel` 窗口/哨兵任务 + `sentinel`/`stage` 记录 + 豁免门控；
   - 诊断档状态机（sentinel emit → 门控翻转 + 规则 reload + 切换完成信号）；
   - wfgen `perf-diag` 子命令（EPS 计算与哨兵文件读取抽成库函数以便单测）。
2. **perf_diag_case `floor` 档 EPS ≥ 10M**（N ≥ 1M 验收）：单流小字段管道吞吐
   应高于 qradar 6 流 floor 实测 9.7M；verify.sh 增断言 1m floor ≥ 10M。

## 9. 落地清单（实现顺序）

1. wf-config：`PerfConfig`（diag/cut_rules/cut_output）+ 解析 + 测试；
2. wf-runtime：`set_perf_cuts` 原子门控 + `process_batch`/`emit` 切口 + 测试；
3. wf-runtime：内置 `__wf_sentinel` 窗口 + 独立哨兵任务 + 哨兵记录落盘
   （走 alert 链、天然不受 cut_output 门控）+ 豁免门控测试；perf_diag_case 侧补
   `topology/sinks/business.d/sentinel.toml`（`data/perf_sentinel.ndjson`）；
4. wf-runtime：诊断档状态机——sentinel emit → 门控翻转 + 规则子集 reload 触发 +
   `perf_stage{current=k}` 切换完成信号（无 admin 端点）；
5. wf-config：`report_interval` 默认 100ms；
6. wfgen：`perf-diag` 子命令（切档 → 发帧+sentinel → 读指标 → 墙表）；
7. 文档：`PERF_BISECTION_METHOD.md` 挂接本机制（§6 定位流程即机制用法）。
