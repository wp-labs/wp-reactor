# 性能诊断模式（perf-diag）使用指南

> 吞吐（EPS）不达标或回归时，用**诊断模式**定位瓶颈在管线哪一段——不用改代码、
> 不用重启 daemon、不手拼测量。机制设计见
> [`../design/perf-diag-mode-design.md`](../design/perf-diag-mode-design.md)，
> 方法论见 [`../PERF_BISECTION_METHOD.md`](../PERF_BISECTION_METHOD.md)。

## 1. 这是什么

诊断模式 = **引擎内置的性能墙定位开关**。把管线按"只有禁止"的方式切成几段：

```text
recv（TCP 接收，非哨兵帧 body 即丢）
decode（+ 解码，窗口 append 前即丢）
floor（+ 窗口 append / fanout）   ← 切掉规则求值 + 切掉输出链
rules（+ 规则求值）               ← 只切输出链
emit（+ 输出构建 + 通道投递）     ← sink 收到即丢（cut_sink_write）
full（+ sink 物化 + 序列化 + 写） ← 什么都不切
```

逐段测出 EPS，增量成本最大的一段就是墙。整个过程：

- 由**启动参数**进入（`wfusion daemon --perf-diag conf/perf-diag.toml`），
  生产不带参数 = 完全关闭，`wfusion.toml` 零污染；
- **诊断档自动切换**：wfgen 每批帧尾追加一条哨兵（漂流瓶），引擎处理完
  "批末最后一条"后自动切到下一个诊断档——单 daemon 一次跑完所有档；
- **EPS 直接可算**：哨兵记录自带 `{round, n, start_ns, emit_ns}` 四元组，
  `eps = n / (emit_ns − start_ns)`，全程无外部记账。`emit_ns` 是引擎等**数据窗
  排空**（每个数据窗 `min_acked ≥ next_seq`，即全部数据被所有规则消费完）后的
  时刻——实现细节见[设计文档 §4.3](../design/perf-diag-mode-design.md)。

## 2. 快速开始（10 秒看墙梯）

自带的独立验证 case `wf-examples/performance/perf_diag_case/` 即开即用：

```bash
# 0. 构建 release 二进制（warp-fusion 仓库）
cd warp-fusion && cargo build --release -p wfusion -p wfgen

# 1. 机制 + 墙梯验证（默认 100k 事件，秒级）
cd wf-examples/performance/perf_diag_case
./verify.sh                 # 或 ./verify.sh 1000000（跑 1M + 10M 门禁）

# 2. 看墙表
cat data/perf_diag_wall.txt
```

典型输出（本机 N=1M）：

```text
floor  eps=57660151 n=1000000 rounds=1
rules  eps=630345   n=1000000 rounds=1
full   eps=641240   n=1000000 rounds=1
```

解读：`floor`（净管道）能到 57.7M/s；加上 21 条触发中的规则掉到 630k/s——
**规则求值就是墙**；输出链（blackhole）几乎无额外成本（rules ≈ full）。

## 3. 三步流程

在任意 bench（nexmark_pk / qradar_pk / 自有负载）上定位：

### 3.1 起诊断 daemon

```bash
wfusion daemon --perf-diag conf/perf-diag.toml --config conf/wfusion.toml --work-dir .
```

启动日志会打印诊断状态：

```text
INFO [sys] perf-diag 诊断模式  stages=3 initial_gates="cut_rules=true cut_output=true"
```

不带 `--perf-diag` 时明示：`perf-diag 未启用（无 --perf-diag）——哨兵帧将按未知流 window miss 丢弃`。

### 3.2 预编码数据帧（一次）

```bash
wfgen dump-frames --scenario scenarios/nexmark.wfg --input data/events.jsonl \
  --addr 127.0.0.1:9800 --ws models/schemas/*.wfs --output data/events.frames \
  --chunk 10000 --max-frame-bytes 8388608 --max-frame-rows 100000
```

帧文件把"JSON 解析 + Arrow 编码"从测量热路径上拿掉——发送时纯字节复制。

### 3.3 驱动诊断

```bash
wfgen perf-diag --diag conf/perf-diag.toml \
  --frames data/events.frames --addr 127.0.0.1:9800 \
  --n-list "1m,3m" --timeout-secs 120
```

逐档输出 + 墙表落盘 `data/perf_diag_wall.txt`。

## 4. 诊断档与墙梯语义

| 档 | 关键门控 | 测得 | 对应二分法的刀 |
|---|---|---|---|
| `recv` | `cut_recv`（非哨兵帧 body 即丢） | TCP 接收（字节率） | ⑤ 读粒度 |
| `decode` | `cut_append`（append 前即丢） | + 解码 | ③ 切解码校验 |
| `floor` | `cut_rules` + `cut_output` | + 窗口 append / fanout | ①② 输出+规则 |
| `rules` | `cut_output` | + 规则求值（规则墙） | ② 切规则 |
| `emit` | `cut_sink_write`（sink 收到即丢） | + 输出构建 + 通道投递 | ① 切输出（构建侧） |
| `full` | 全开 | + sink 物化 + 序列化 + 写 | ① 切输出（写侧） |

- **叠加式**：后一档 = 前一档 + 该段成本；每档 EPS 的差 = 该段的增量成本
  （如 `full − emit` = 序列化 + sink 写成本）；
- **两个"序列化"是不同段（2026-08-26 统一命名）**：worker 侧的
  `alert.append_nanos`（`AlertColumnBuilder::append_record` 的 record→列构建，属
  **输出构建**段，emit 档内）vs sink 侧的 `cut_sink_write` 门控（列→行物化 + JSON
  编码 + 写盘，属 **full − emit** 增量）。门控叫 `cut_sink_write`（旧名
  `cut_serialize`）只切 sink 侧；worker 侧指标叫 `append_*`（旧名
  `serialize_*`）。
- **每档测一次**：哨兵驱动的切换在首个哨兵后即发生，同档重复轮次会吃到下一档
  门控——去噪用 `--n-list` 递增 N，不要用 `--rounds`；
- **数据由小到大**：小 N 秒级出方向，大 N 确认墙是 per-event（随 N 线性）还是
  固定开销（与 N 无关）。

## 5. 配置参考（`perf-diag.toml`）

```toml
# 入口是 --perf-diag 启动参数本身；本文件只承载诊断档列表
[[stages]]
name = "recv"
cut_recv = true          # 只读帧头 tag，非哨兵帧 body 即丢

[[stages]]
name = "decode"
cut_append = true        # 解码后、窗口 append 前即丢

[[stages]]
name = "floor"
cut_rules = true
cut_output = true
rules = ""               # 空 = 保持当前规则；否则规则文件路径（触发热 reload）

[[stages]]
name = "rules"
cut_rules = false
cut_output = true

[[stages]]
name = "emit"
cut_sink_write = true    # sink 收到 AlertBatch 即丢（不物化/序列化/写）

[[stages]]
name = "full"
cut_rules = false
cut_output = false
cut_sink_write = false
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `[[stages]].name` | string | 档名（墙表输出用） |
| `[[stages]].cut_rules` | bool | 禁止规则求值 |
| `[[stages]].cut_output` | bool | 禁止整条输出链（record→列构建 + 通道 + sink 物化/序列化/写） |
| `[[stages]].cut_append` | bool | 禁止窗口 append（解码后即丢；哨兵流豁免） |
| `[[stages]].cut_recv` | bool | 禁止解码（只读帧头；哨兵流豁免） |
| `[[stages]].cut_sink_write` | bool | 禁止 **sink 消费侧**（列→行物化 + 序列化 + 写盘；AlertBatch 到 sink 即丢）——与 worker 侧 `append_*` 指标（record→列构建）区分，见 §4 |
| `[[stages]].rules` | string? | 规则子集文件路径；非空且不同 → 热 reload（不加钱换配置） |

启动即应用 `stages[0]` 的门控；`--perf-diag` 不带 = 全关（生产零污染）。

### 5.1 诊断模式内存口径（`WF_DIAG_MAX_TOTAL_BYTES`）

诊断模式启动时，引擎把全局窗口内存 cap（`window_defaults.max_total_bytes`）覆盖为
`max(配置值, 计算值)`——**墙梯把同一份数据重发 N 档会放大窗口内存压力**，cap 过小
会让 `commit_append` 停车，把内存墙错报成计算墙（q20 实测 2GB 下 rules 假墙
+324ns/事件，8GB 下真值 −0.5ns）。

| 取值 | 效果 |
|---|---|
| （未设） | 物理内存 × 60%（通用方案，按机器比例放量） |
| `"8GB"` / `"4096MB"` | 显式字节 cap |
| `"60%"` | 物理内存 × 百分比 |
| `"0"` | 不覆盖，沿用配置文件（测生产内存约束口径） |

- **仅诊断模式读取**；非诊断模式一律按标准配置走，生产零污染；
- 诊断只放大不缩小（取 max）；单窗 `max_window_bytes` 不变；
- 启动日志打印实际口径 `perf-diag 内存口径: max_total_bytes=…`；bench 的
  `diag.sh` 自动抓进报告口径行（`data/diag_<q>_<total>.txt` 末尾「口径:」）。
  报告里看不到该行 = 二进制旧、未应用放量——重建 `wfusion` 再跑。

## 6. 命令参考

### `wfusion daemon [--perf-diag <path>]`

| 参数 | 说明 |
|---|---|
| `--perf-diag <path>` | 诊断配置（同上表）；不带 = 全关，生产零污染 |
| `--config` / `--work-dir` | 常规配置，与无诊断时一致 |

### `wfgen perf-diag`

| 参数 | 缺省 | 说明 |
|---|---|---|
| `--diag <path>` | 必填 | 与 daemon **同一份** `perf-diag.toml`；`[[stages]]` 列表 = 轮数 |
| `--frames <file>` | 必填 | 预编码帧（`dump-frames` 产物，须覆盖 max(`--n-list`) 行） |
| `--addr host:port` | `127.0.0.1:9800` | TCP 数据端口 |
| `--n-list "100k,1m,3m"` | 帧全部行 | 每档按递增 N 各测一次 |
| `--rounds N` | `1` | 保留参数；语义见 §4（实际每档仅首轮有效） |
| `--sentinels <file>` | `data/perf_sentinel.ndjson` | 哨兵记录文件 |
| `--output <file>` | `data/perf_diag_wall.txt` | 墙表输出 |
| `--timeout-secs N` | `60` | 单次等待（切换/哨兵记录）超时 |

## 7. 在真实 bench 上启用

以 nexmark_pk 为例，需要 3 处（每 bench 一份，独立于基准数据）：

1. **`conf/perf-diag.toml`** — 诊断档列表（§5 模板）；
2. **哨兵记录 sink** — `topology/sinks/business.d/sentinel.toml`（**`wfadm init` 已自动生成**，无需手写）：

   ```toml
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

   ⚠ 必须放 **`business.d/`**（`infra.d` 只读 `default/error/monitor` 三个固定文件）；
   新 case 用 `wfadm init` 初始化即自带（业务路由组，窗口匹配加载）；
3. **规则输出 sink** — 已有（benchmark blackhole 组）即覆盖；无则加一个
   `windows = ["<yield 目标>"]` 的组（否则 full 档告警无 sink，输出被丢弃并告警）。

然后按 §3 三步跑。

> **非诊断用途（精确 EPS 统计）**：哨兵四元组本身也是精确的**完成信号/EPS 口径**。
> nexmark_pk `bench.sh` 已接入：daemon 带 `--perf-diag`（**无档**配置 = 门控全
> false，性能零影响，仅注册 `__wf_sentinel` 窗口）启动，发送端
> `wfgen send-arrow/stream --sentinel` 启用**分连接哨兵**——每条连接 copy 完
> 自己的数据后追加哨兵帧（round=连接号, n=该连接实际行数），单连接 1 条
> （round=0）；引擎等**数据窗排空**后写四元组。完成判定与 EPS 直接读
> `perf_sentinel.ndjson`（多连接汇总 Σn/(max emit − min start)，各连接 dt 可
> 对比做均衡诊断），无 metrics 轮询（±200ms）粒度误差。任意 bench 的发送端加
> `--sentinel` 即可复用。

## 8. 墙表解读

`data/perf_diag_wall.txt` 每行：`<档名>  eps=<EPS> n=<发送量> rounds=<轮数>`。

```text
floor  eps=57660151 n=1000000 rounds=1
rules  eps=630345   n=1000000 rounds=1
full   eps=641240   n=1000000 rounds=1
```

- 档间 EPS 差 = 该段增量成本（`floor→rules` 掉 57.6M→0.63M = 规则墙）；
- 墙判定：CPU 高且增量大 = 忙墙（计算密集）；CPU 低 = 等/供给墙（I/O、预算槽位）——
  采样 daemon CPU% 判别（方法论 §2.4）；
- `full` 档加真实 sink 后（非 blackhole）可测输出链成本；blackhole 口径下
  `rules ≈ full` 属正常（输出近无成本）。

## 9. 排障 FAQ

| 现象 | 原因 | 处理 |
|---|---|---|
| `timeout waiting for stage{current=0}` | daemon 未带 `--perf-diag`（哨兵帧被当未知流丢弃）；或哨兵文件在 daemon 启动后被清空 | 先起 daemon 再 `rm -f data/perf_sentinel.ndjson`；确认启动日志含"诊断模式" |
| `timeout waiting for sentinel{round=k}` | 数据窗未排空（规则慢/挂起），或哨兵记录未落盘 | 看 daemon 日志 window miss / 规则超时；`--timeout-secs` 调大 |
| 墙梯无区分度（floor ≈ full） | 事件时间太疏：窗口（如 2m）内同 key 计数上不去，规则不触发 | 把生成数据的时间步进改密（perf_diag_case 用 1ms，同 key 1s 一条） |
| `rules` 比 `full` 快/慢 | 输出墙小（blackhole），rules≈full 在 ±15% 噪声内 | 加大 N 或多次取 max；输出墙容差放宽 |
| 墙梯出现负增量档 / 低 CPU 假墙（如 rules 档 CPU 只有个位数 %） | 全局窗口内存 cap 太小，gate 停车把内存墙错报成计算墙（q20 实证） | 确认报告口径行显示 cap=物理内存 60%（`WF_DIAG_MAX_TOTAL_BYTES` 可调大或减 N）；旧二进制要重建 |
| 首轮后同档轮次变慢 | `--rounds > 1`：首个哨兵已切下一档，后续轮次吃新门控 | 用 `rounds=1` + `--n-list` 递增去噪 |
| 哨兵记录 `start_ns`/`emit_ns` 是字符串 | 设计如此：epoch nanos 超 f64 精确范围，字符串保精确 | 解析回 i64 再算 EPS（wfgen 已处理） |
| 跨机部署 EPS 偏差大 | wfgen 与引擎时钟不同机，`start_ns`/`emit_ns` 不可比 | 诊断须同机跑；跨机需 NTP 或引擎回写差值（未做） |

## 10. 限制

- **单 daemon 不重启**：全档一次跑完（含规则子集热切）；`budget:X`（preread 预算）
  档是唯一重启例外（RequiresRestart）；
- **哨兵走独立窗口**：测共享段（recv/decode/路由）+ 数据窗排空，绝对时间可能略早
  于最慢数据窗的规则消化完，但**跨档同一口径**，增量墙归属判定不受影响；
- **`__wf_sentinel` 是保留名**：诊断模式下用户不得在 `.wfs`/`windows.toml` 声明
  同名窗口（启动即报错）。
