# wp-model ↔ Arrow 类型映射规格表（三方口径）

> **状态：现行口径快照（2026-09）**
>
> 本文是 **wparse（sink 侧）↔ wfusion（接收侧）** 之间 Arrow 列类型契约的**单一事实来源**。
> 三处手工映射表的**唯一权威版本**就是下面的规格表；任何一处改动都必须同时更新本文档与对应的钉桩测试。
>
> 姊妹文档：[`arrow-tcp-stream-compatibility.md`](./arrow-tcp-stream-compatibility.md)（IPC 帧/framing 口径，已实现）。

## 1. 现状：契约表已归位 `wp-arrow`，值层仍待合并

同一份 `wp_model_core::model::DataType` 的列类型映射，当前只有**一处在生效**：

| 角色 | 位置 | 输入类型 | 覆盖 | 在线上吗 |
|---|---|---|---|---|
| **A. 期望侧（线协议契约）** | `crates/wf-runtime/src/receiver/schema.rs` `field_type_to_arrow` | `wf_lang::FieldType`（WPL 窗口声明） | 7 个 `BaseType` + `Object` / `ArrayAny` / `Array(BaseType)` | ✅（经 B 推导，见 A-1） |
| **B. sink 侧（生产者）** | `crates-wp/wp-connector-utils/src/arrow/schema.rs` `wp_type_to_arrow` | `wp_model_core::model::DataType` | **再导出 C**（公开路径不变，见 A-2 2b） | ✅ |
| **C. `wp-arrow` 的契约表** | `wfusion/wp-arrow/src/contract.rs` `wp_type_to_arrow` | `wp_model_core::model::DataType` | **穷尽 37 个变体**（新增变体会编译失败）+ 口径钉桩 | ✅ **契约的唯一实现**（A-2 2b 后经 B 再导出进入生产；`wp-arrow 0.4.1`） |
| **C′. `wp-arrow` 的类型化前端** | `wfusion/wp-arrow/src/{schema,convert}.rs` | `wp_arrow::schema::WpDataType`（9 变体） | schema 9 变体 + 一整套值/列转换 | ❌ 不在生产路径（见 A-0） |

### C′（类型化前端）的定性（A-0，2026-09-19 实测；当晚复核更正）

全家族（wp-reactor / warp-fusion / crates-wp）里 `wp_arrow::` 的引用共 **31 处**：`ipc` 30、`schema` 1、`convert` **0**。
而这 31 处**没有一处在生产路径上**（**注意：这是 A-2 2b 之前的快照**）：

- `ipc`（`encode_ipc` / `decode_ipc`）的 30 处 —— 全部位于测试或文档注释：`wf-runtime` 的 `mod tests`
  （`receiver/arrow.rs`、`source/mod.rs`）、`receiver/tests.rs` 用 `#[path]` 挂载的
  `receiver_tests_arrow_coerce`、以及 `wfgen/tests/*` 集成测试；生产代码里提到它的只有
  `receiver/arrow.rs:22` 的一行注释。
- `schema` 的 1 处 —— 本仓的参考对拍 `arrow_contract_matches_wp_arrow_on_overlap`。
- **`convert`（`records_to_batch` / `batch_to_records`）0 处** —— 但它是一份**完整的**
  wp-model↔Arrow 值/列转换实现，与 B 的 `arrow/record.rs` 职责重复。

> **更正记录（2026-09-19）**：本节原写「`wp_arrow` 在生产代码里**只有 `ipc` 模块被使用**（全家族 30 处）」。
> 复核后：那 30 处**全是测试/文档引用**，生产调用为 **0**。写成「只有 ipc 被使用」仍暗示了一个
> **并不存在**的生产依赖 —— 而 C′ 被当成契约方，正是 `Hex` 分叉被误判的根因（我当时也这样误判过）。
>
> **A-2 2b 之后**：`wp-arrow` 的 `contract` 模块**已进入生产路径**（经 B 的再导出），
> 上表 C′ 那两行（`schema` / `convert`）仍不在路径上。

> `Ip` / `Hex` 等值在 C 里走 `Utf8`、`Array` 走 `List(inner)`、`BigInt` 走 `Decimal256`，都是**它自己的口径**，
> 与线协议无关。**不要动 `WpDataType::Digit` 这个名字**：它是 `wp-arrow` 自己的 9 变体枚举，
> 家族那轮 `Digit → Int` 正名刻意没触及它（改它会同时改字段元数据字符串与行为）。

### 收敛步骤 1（A-1）：两张活表变一张 —— 已完成 2026-09-19

A 与 B 的**输入类型不同**（WPL 类型 vs `wp_model_core::DataType`），所以它们不是重复实现，而是同一契约的两端。
A-1 把 **B 的表变成唯一实现**，A 改为 `WPL 类型 → wp_model_core::DataType → B 的表`：

```
WPL FieldType ──(7 臂 + 结构化)──▶ wp_model_core::DataType ──(B：穷尽 37)──▶ Arrow
```

落地方式：`wp-connector-utils 0.3.2` 公开 `wp_type_to_arrow`（纯新增 API），
`wf-runtime` 新增直接依赖、让 `field_type_to_arrow` 只做一次转发 —— 两张活表变一张，
`Hex` 那类分叉**从结构上不可能再发生**（一改，A 自动跟随）。

顺带补上一个真实缺口：B 自己的 CI 原先**抓不到**它引入的分叉（契约测试只活在 wp-reactor 那边），
现在由 `arrow_contract_expected_column_is_pinned`（钉生效口径）加跨仓端到端用例共同兜住。

**这一步的收益与落点无关，必须保留**：下面步骤 2 只是把这张唯一表**换个 crate**，A 依然「自动跟随」。

### 收敛方向（A-1）：已完成 2026-09-19

A 与 B 的**输入类型不同**（WPL 类型 vs `wp_model_core::DataType`），所以它们不是重复实现，而是同一契约的两端。
A-1 把 **B 的表变成唯一实现**，A 改为 `WPL 类型 → wp_model_core::DataType → B 的表`：

```
WPL FieldType ──(7 臂 + 结构化)──▶ wp_model_core::DataType ──(B：穷尽 37)──▶ Arrow
```

落地方式：`wp-connector-utils 0.3.2` 公开 `wp_type_to_arrow`（纯新增 API），
`wf-runtime` 新增直接依赖、让 `field_type_to_arrow` 只做一次转发 —— 两张活表变一张，
`Hex` 那类分叉**从结构上不可能再发生**（B 一改，A 自动跟随）。

顺带补上一个真实缺口：B 自己的 CI 原先**抓不到**它引入的分叉（契约测试只活在 wp-reactor 那边），
现在由 `arrow_contract_expected_column_is_pinned`（钉生效口径）加跨仓端到端用例共同兜住。

> A↔C 的比较**保留为参考**（`arrow_contract_matches_wp_arrow_on_overlap`）：C 当前不在契约里（A-0），
> 但 A-2 落地后它就是唯一实现（这份对拍届时升级为「契约口径自洽」检查）。

### 收敛步骤 2（A-2，目标态）：唯一实现的落点从 B 迁到 C —— 第 1 步已落地，第 2 步待发版

**为什么还要搬**：B 是「面向 sink 的 connector 工具」crate，把**线协议契约**放在那里是**定位倒置**；
C（`wp-arrow`）的名字与职责就是「wp-model ↔ Arrow」，它**本就依赖 `wp-model-core`**，
且已持有值/列转换（`convert.rs`）——契约的语义归属在 C。
#102 自己的建议也是「合并为单一实现（由 `wp-model-core` 提供，**或由 `wp-arrow` 独占**）」，
A-1 选的是「改动最小」的第三种。（不选 `wp-model-core`：那会让模型层依赖 `arrow`，把基础 crate 与列式库绑死。）

**目标态**（A、B 都改为调用 C 的同一条入口，输入统一到 `wp_model_core::DataType`）：

```
WPL FieldType ──▶ wp_model_core::DataType ──(C：穷尽 37)──▶ Arrow
```

**进度**：

| 步 | 内容 | 状态 |
|---|---|---|
| **2a** | C 新增 `contract::wp_type_to_arrow`（穷尽 37 变体，从 B **逐字搬迁**） + 同一份全变体钉桩测试 + 防呆两条（契约与类型化前端的**差异行**与**重叠行**各一条） | ✅ 2026-09-19（`wp-arrow/src/contract.rs`） |
| **2b** | B 改为**转发/再导出**到 `wp_arrow::contract::wp_type_to_arrow`，删掉自己的表与 37 变体钉桩（保留消费侧冒烟） | ✅ 2026-09-19（`wp-connector-utils` **0.3.4**；实现用 `pub use` 而非包装函数 → 路径不变且**结构上**不可能漂移。0.3.3 的依赖范围写法有误，见 §5 的教训框） |
| **2c** | 契约的**值层**（`DataRecord` → 列）迁入 C | ✅ 2026-09-19（`wp-arrow 0.4.2` + `wp-connector-utils 0.3.5`；见下方「2c 的实做法」） |

> 2a 是**纯新增**，2b 是**纯转发**（公开路径与签名不变）——两者都不改行为。
> **2b 之后契约的生效实现已经是 C**（`wp-arrow`），§3 的读法随之更新。

#### 2c 的实做法（与计划的一处差异，故意）

原计划写的是「C′ 的 `convert.rs` 与 B 的 `arrow/record.rs` 合成一份」。实际做法不同：

- 把 **B 的值层语义**（`record.rs` 的列构造与回退规则）逐字移植为 **`wp_arrow::contract::value`**
  （新模块，与列类型表同层）；B 改为转发。
- **没有动 C′ 的 `convert.rs`**：它是**类型化前端**的值层，口径本就不同（`Array` → `List`、
  `BigInt` → `Decimal256`），把它「合并」进来只会两个结局：要么改线上字节，要么改它的公开 API。
  所以它保持原样（仍 0 调用、非契约），只是与 `contract/value.rs` 在文档上分开。
- 错误类型：`SinkResult` / `SinkReason` → `WpArrowError`（C 不能依赖 `wp-connector-api`，否则依赖方向再次倒置）。

**等价性凭据**（而不是「看着一样」）：同一份**金标准**夹具与期望（全列类型 + 缺字段 + Chars/Int/Float/Time
互转 + Hex·Binary + 结构化 JSON + null）在**迁移前**（B 的旧实现）与**迁移后**（`contract::value`）
两处**各有一份且同时通过**：

- 实现侧：`wp-arrow` 的 `contract::value::tests::wire_value_encoding_is_pinned_by_golden_values`
- 消费侧：`wp-connector-utils` 的 `arrow::record::tests::wire_value_encoding_is_pinned_by_golden_values`

两份是**逐字同构**的（夹具与断言一致）；搬迁后都保留 —— 任一侧改口径都会先报一次。

**要点（评审已确认，搬迁时按此执行）**：

1. 入参就用 `wp_model_core::model::DataType`，**不需要把 `WpDataType` 从 9 变体扩到 37** ——
   只是把函数搬过去；C 现有的 `WpDataType` API 保留为「类型化前端」（9 变体仍是它自己的口径）。
2. **值层必须一起搬**，否则契约仍被劈成两半（schema 在 C、值编码在 B）：
   C 的 `convert.rs` 与 B 的 `arrow/record.rs` 要合并成一份。
3. **错误类型要解耦**：B 的 `record.rs` 返回 `SinkResult`/`SinkReason`（来自 `wp-connector-api`），
   而 C 不能依赖 connector crate（依赖方向会再次倒置）—— 改为 `WpArrowError` 或泛型化。
4. **跨仓发布顺序**：C 加 API → 发版 → B 改为转发（`0.3.x`）→ A 跟随 → 删重复实现。
5. **桩测试与本文档同 PR 迁移**（§5 的流程不变，只是「唯一实现」位置变了）。

**为什么现在做最便宜**：C 当前 0 生产调用，改它风险≈ 0；一旦它成为契约实现，API 稳定性要求会显著提高。

> 判别标准（记进 §6）：**「契约的唯一实现」住在哪个 crate，那个 crate 就该是 `wp-arrow`** ——
> 否则 #102 这类「按自我声明去找权威实现 → 找到的是另一份表 → 报不一致」的误判会继续发生。

## 2. 线协议兼容性判定规则（A 的收口口径）

接收侧不是逐列严格相等，而是走 `receiver/schema.rs:32` `schemas_are_compatible_for_stream`：

1. 列数必须相等（`:36`）。
2. 列名必须相等（`:47`）。
3. 逐列类型：
   - 该列若是**结构化字段**（带 `wfl_field_type` 元数据，`wf_lang::wfl_structured_field_kind` 返回 `object`/`array`），
     则走 `:56` `structured_field_is_compatible`：
     - `Utf8` + 同种 kind → 兼容；`Utf8` 而**无** kind 元数据 → **兼容**（`:60`）；
     - `Struct(_)` 仅当期望 `object` 时兼容（`:62`）；
     - `List(_)` / `LargeList(_)` / `FixedSizeList(_,_)` 仅当期望 `array` 时兼容（`:63`）；
     - 其它（含 `Binary`）→ **不兼容**（`:66`）。
   - 否则**要求 `expected == actual` 严格相等**（`:52`）。
4. 任一列不兼容 → `:14` `validate_batch_schema_for_stream` 抛
   `RuntimeReason::data_error()`：`arrow source schema mismatch for stream "…"`。

**即使绕过校验，也不要指望被救回来**：`receiver/route.rs` `coerce_column` 的源类型白名单是
`Utf8` / `Int64` / `Int32` / `Float64` / `Boolean` / `Timestamp(Ns)`（2026-09-19 扩充，此前只有前三种），
其余（`Binary` / `List` / `Struct` / `Decimal` …）→ **列整置 null**（值丢失）。

> **兜底不再静默，也不再是 `NullArray`（2026-09-19 修）**：旧实现返回 `NullArray`，它有两个问题 ——
> ① 类型是 `DataType::Null`，与目标字段不符 → `RecordBatch::try_new` 失败 →
> `project_batch_for_stream` 把失败吞成「返回未投影的批」→ **整个投影静默作废**；
> ② `NullArray::null_count()` 恒为 **0**，用 null 计数判断会误读成「没丢值」。
> 现在按**目标类型**产全 null 列（`new_null_array`）并 `tracing::warn!`，
> 所以：列类型正确、`null_count()` 如实（= 行数）、日志可搜「`receiver: 列类型无法转换`」；
> 批构造失败与缺字段补全也同样留 WARN。**结论不变：数据救不回来，但不再静默。**

## 3. 规格表：`wp_model_core::model::DataType`（37 变体）× 三方

图例：`n/a` = 该侧的表达能力**无法表示**此类型。

> **A-2 2b 之后（2026-09-19）这张表的读法**：
> **C 列是唯一实现**（`wp_arrow::contract::wp_type_to_arrow`，穷尽 37 变体，口径钉桩在 `contract::tests`）；
> **B 列是 C 的再导出**（`pub use`，同一函数，不是副本）；
> **A 列不再是自己一张表**，而是 `WPL 类型 → wp_model_core::DataType → B/C` 推导出来的 ——
> 所以 A/B/C 在 `Base` 与结构化行上**必然一致**（值保留在此仅供核对）。
> C′（`WpDataType` 类型化前端）不在契约上，它的 `Array` → `List(inner)` / `BigInt` → `Decimal256`
> 是它自己的口径（规格表 §4 DIV-2·DIV-3）。

| # | DataType | serde | A 期望侧（WPL，推导） | B wp-connector-utils（再导出 C） | C wp-arrow「contract」（唯一实现） |
|---:|---|---|---|---|---|
| 1 | `Bool` | `bool` | `Boolean` | `Boolean` | `Boolean` |
| 2 | `Chars` | `chars` | `Utf8` | `Utf8` | `Utf8` |
| 3 | `Symbol` | `symbol` | n/a | `Utf8` | n/a |
| 4 | `PeekSymbol` | `peek_symbol` | n/a | `Utf8` | n/a |
| 5 | `Int` | `int` | `Int64` | `Int64` | `Int64` |
| 6 | `BigInt` | `bigint` | n/a | `Utf8` ⚠ | `Decimal256(39,0)` ⚠ |
| 7 | `Float` | `float` | `Float64` | `Float64` | `Float64` |
| 8 | `Ignore` | `ignore` | n/a | 不入 schema（单独映 `Utf8`） | n/a |
| 9 | `Time` | `time` | `Timestamp(ns,None)` | `Timestamp(ns,None)` | `Timestamp(ns,None)` |
| 10 | `TimeISO` | `time_iso` | n/a | `Timestamp(ns,None)` | n/a |
| 11 | `TimeRFC3339` | `time_3339` | n/a | `Timestamp(ns,None)` | n/a |
| 12 | `TimeRFC2822` | `time_2822` | n/a | `Timestamp(ns,None)` | n/a |
| 13 | `TimeTIMESTAMP` | `time_timestamp` | n/a | `Timestamp(ns,None)` | n/a |
| 14 | `TimeCLF` | `time_clf` | n/a | `Timestamp(ns,None)` | n/a |
| 15 | `IP` | `ip` | `Utf8` | `Utf8` | `Utf8` |
| 16 | `IpNet` | `ip_net` | n/a | `Utf8` | n/a |
| 17 | `Domain` | `domain` | n/a | `Utf8` | n/a |
| 18 | `Email` | `email` | n/a | `Utf8` | n/a |
| 19 | `Port` | `port` | n/a | `Int32` | n/a |
| 20 | `SN` | `sn` | n/a | `Utf8` | n/a |
| 21 | `Hex` | `hex` | `Utf8` | `Utf8` | `Utf8` |
| 22 | `Base64` | `base64` | n/a | `Binary` | n/a |
| 23 | `KV` | `kv` | n/a | `Utf8` | n/a |
| 24 | `KvArr` | `kvarr` | n/a | `Utf8` | n/a |
| 25 | `Json` | `json` | n/a | `Utf8` | n/a |
| 26 | `ExactJson` | `exact_json` | n/a | `Utf8` | n/a |
| 27 | `HttpRequest` | `http_request` | n/a | `Utf8` | n/a |
| 28 | `HttpStatus` | `http_status` | n/a | `Utf8` | n/a |
| 29 | `HttpAgent` | `http_agent` | n/a | `Utf8` | n/a |
| 30 | `HttpMethod` | `http_method` | n/a | `Utf8` | n/a |
| 31 | `Url` | `url` | n/a | `Utf8` | n/a |
| 32 | `Auto` | `auto` | n/a | `Utf8` | n/a |
| 33 | `ProtoText` | `proto-text` | n/a | `Utf8` | n/a |
| 34 | `Obj` | `obj` | `Utf8` + meta(`kind=object`) | `Utf8`（**无** meta） | n/a |
| 35 | `Array(subtype)` | `array` | `Utf8` + meta(`kind=array`) | `Utf8`（**无** meta） | `List(inner)` |
| 36 | `IdCard` | `id_card` | n/a | `Utf8` | n/a |
| 37 | `MobilePhone` | `mobile_phone` | n/a | `Utf8` | n/a |

## 4. 已知差异（DIV）

| 编号 | 类型 | 分歧 | 严重度 | 处置 |
|---|---|---|---|---|
| **DIV-2** | `BigInt` | B = `Utf8`（十进制字符串），C = `Decimal256(39,0)`（数值） | 已降为**无影响** | **本轮登记不修**，见下 |
| **DIV-3** | `Obj` / `Array` | B 给 `Utf8` 且**不带** `wfl_field_type` 元数据；A 给 `Utf8`+meta；C 给 `List(inner)` | 有意设计（“容忍”） | **本轮登记不修**，见下 |
| **DIV-4** | 覆盖范围 | 7 / 37 / 9 个变体 | 结构性 | 由 **A-1** 收敛为一张活表 |

### DIV-2（`BigInt`）：零生产者，本轮不修

2026-09-19 实测：**家族内没有任何东西在生产 `DataType::BigInt` / `Value::BigUint`** ——

- WPL 无法声明 `bigint`（`wf-lang::BaseType` 只有 7 种，`wp-parse-api` / `wp-core-connectors` 里无 `"bigint"` 类型名）；
- `wp-connectors/src/dmdb/source.rs` 里的 `DataType::BigInt` 是 **DM 驱动自己的**类型枚举，不是 `wp_model_core` 的（同名不同物）；
- 生态现有的写出口径一律是**字符串**：doris sink `Value::BigUint(v) => serialize_str(v.to_string())`、
  `wp-data-fmt/src/json.rs` 同样写 JSON 字符串。

再加上 C 不在线协议路径上（§1），所以这条分叉**当前零影响**。
若将来真有生产者：建议统一为**十进制字符串**（与生态现有口径一致、且改 C 一行即可），而不是把 B 改成 `Decimal256`
（后者要给 `wp-connector-utils` 新增 Decimal256 builder，而接收侧根本无法声明 bigint）。

### DIV-3（结构化字段元数据）：容忍是有意设计，本轮不修

接收侧对「`Utf8` 但无 `wfl_field_type` 元数据」返回**兼容**（`structured_field_is_compatible`），
而且这条路径有**明确的测试祝福**：
`wf-runtime` 的 `route_projects_plain_utf8_json_into_structured_window_schema` 用的就是一个无元数据的普通 `Utf8` 字段，
直接投影进 `Object` 窗口 —— 即“JSON 字符串 + 目标窗口声明”就是设计中的传输形态，语义在 coerce 阶段兑现。

**要不要显式化（让 B 附上 `wfl_field_type`）？** 本轮不做，因为：

1. `wp-connector-utils` 必须**硬编码** `"wfl_field_type"` 与 `"object"/"array"` —— 而这些常量现在住在 `wf-engine`
   （wp-reactor 的 crate）。这等于拿一条**新的跨仓隐式契约**换掉现有的容忍，得先把常量提到共享位置（属 A-1 的后续）；
2. 它会把行为从“容忍”变“严格”，可能拒掉现在能过的组合（需要先盘点存量）。

代价已知且可接受：schema 层无法校验结构化语义，要等 coerce 解析 JSON 时才知道（错形会落 null）。

### DIV-1（P0，`Hex`）：已修复（2026-09-19）

**当时的症状**：sink 侧把 `hex` 映为 `Binary`（值域是最小大端原始字节，如 `[0x1A, 0x2B]`），
而期望侧要求 `Utf8`；`hex` 是基础类型、不带结构化元数据 → 走严格相等判定（`schema.rs:52`）
→ `validate_batch_schema_for_stream` 抛 `arrow source schema mismatch for stream "…"`；
即便绕过校验，`coerce_column` 的兜底也会把该列整置 null（**可观测**：typed null + WARN，见 §2）。

**修法：`wp-connector-utils` 的 schema 只改一行。**

```rust
// 前: WpDt::Hex | WpDt::Base64 => DataType::Binary,
// 后: WpDt::Hex => DataType::Utf8,   WpDt::Base64 => DataType::Binary,
```

之所以一行就够（**值层不用改**）：`hex` 改走 Utf8 后，值层落回 `format_utf8_value`，
而 `Value::Hex` 的 `Display` 本 就是 `format!("{:#X}", h.0)`（`wp-model-core` `primitive.rs:8`），
与 `wp-arrow` 的 `convert.rs:498` **逐字一致** —— 三方本来就约定同一个字符串形态，分叉只在 schema 那一行。

**守卫**（防任一侧退回）：

- `wf-runtime` `receiver/schema.rs::arrow_contract_hex_only_accepts_utf8`：`Binary` 仍必须被拒；
- `wf-runtime` `receiver/schema.rs::arrow_contract_hex_utf8_is_accepted`：`Utf8` 正例；
- `wp-connector-utils` `arrow/schema.rs::hex_maps_to_utf8` + `arrow_contract_full_mapping_is_pinned`；
- `wp-connector-utils` `arrow/record.rs::hex_column_uses_the_same_string_form_as_wp_arrow`
  —— **值层对拍**：列里写出的字符串必须等于 `format!("{:#X}", 0x1A2Bu128)`。

> **生效路径（已完成）**：`wp-connector-utils` 是 crates.io 已发布 crate，所以本修复必须发一个
> `0.3.x` **patch** 才能到达 wp-reactor。已发 **0.3.1**（tag `v0.3.1`），并在本仓
> `cargo update -p wp-connector-utils` 跟进。因各方依赖写的是 `^0.3`，**不需要**改动任何上游
> crate 的版本要求 —— 跨仓修「已发布 crate 的行为」时这是最省事的一条路径。
> 跨仓端到端已由 `receiver/schema.rs::arrow_contract_sink_inferred_hex_schema_passes_the_receiver`
> 钉住（临时回退 lock 到 `0.3.0` 实测会失败于 `Binary` vs `Utf8`）。

## 5. 修改流程（必读）

改动任何一侧的类型映射时，**同一个 PR/提交**内必须完成：

1. 改代码。**只改 C**（`wp-arrow/src/contract/mod.rs` 的列类型表、`contract/value.rs` 的值层）——
   B 是转发、A 由降级表推导，两者都会自动跟随。
   （A-2 2b 之前是「只改 B」；再早是两张活表。）
   若确实需要动 A，那只能是改 **`WPL 类型 → wp_model_core::DataType` 的降级表**
   （`wf-runtime/src/receiver/schema.rs` 的 `base_type_to_model` / `field_type_to_model`）。
2. 更新本文档 §3 对应行、§4 的差异表（差异消失就删除该行，不要留「历史差异」）。
3. 更新钉桩测试：
   - C（唯一实现）：`wfusion/wp-arrow/src/contract.rs` → `wire_contract_full_mapping_is_pinned`
   - B（再导出，消费侧冒烟）：`crates-wp/wp-connector-utils/src/arrow/schema.rs`
   - A：`crates/wf-runtime/src/receiver/schema.rs` → `arrow_contract_expected_column_is_pinned`
     与 `arrow_contract_wpl_types_lower_to_model_types`
4. **改 C 要发 `wp-arrow`、改 A 要过 wp-reactor 的 CI**：C 是已发布 crate（改它需发 `0.4.x`），
   然后 B 随 `^0.4` 自动跟进（若动到 B 本身则再发 `0.3.x`，本仓 `cargo update -p wp-connector-utils`；
   本仓 `Cargo.lock` 不跟踪，CI 自行解析）。
   跨仓回归靠 `arrow_contract_sink_inferred_hex_schema_passes_the_receiver`（走真实生产者路径 + 接收侧校验）。

   > ⚠️ **依赖要求必须写「最低可用版本」（0.3.4 的教训）**：B 依赖 C 的**新 API** 时，
   > 不能写只含 major.minor 的宽松形式（`wp-arrow = "0.4"` 按 caret 语义允许解析到 **0.4.0**，
   > 而 `contract` 模块 0.4.1 才有 → 消费方 `Cargo.lock` 停在 0.4.0 时直接
   > `unresolved import wp_arrow::contract` 编不过）。要写 `"0.4.1"`。
   > 对应的升级动作也因此是**两步**（cargo 的 `-p` 更新是保守的）：
   > `cargo update -p wp-arrow --precise 0.4.1` 然后 `cargo update -p wp-connector-utils`，
   > 或直接 `cargo update`。0.3.3 因这个写法应被 yank（仅影响存量 lock 的消费方）。
5. 「差异存在」这件事本身由测试表达：能够容忍的差异写 `assert_ne!` 并注明 DIV 编号。

## 6. 现状与后续

| 项 | 状态 |
|---|---|
| 输出本文档（三方规格表 + 已知差异登记） | ✅ 2026-09-19 |
| B 全 37 变体钉桩 | ✅ `wp-connector-utils/src/arrow/schema.rs` |
| 修 DIV-1（P0，`Hex`） | ✅ 已修复并生效（`wp-connector-utils` 0.3.1 已发布） |
| 消 P0-1（IPC 多批次帧只读第 1 批） | ✅ 已修复（`wf-runtime` `decode_ipc_trusted` 全量解出） |
| **A-0：C 定性** | ✅ 2026-09-19（§1；当晚**复核更正**：`ipc` 30 处 + `schema` 1 处**全在测试/文档**里，`convert` 0 处 —— 生产调用 **0**） |
| **A-1：两张活表变一张** | ✅ 2026-09-19（`wp-connector-utils` 0.3.2 公开 `wp_type_to_arrow`；A 改为由 B 推导） |
| **A-2 第 1 步（2a）：C 新增契约表（`contract` 模块）** | ✅ 2026-09-19（`wp-arrow 0.4.1` 已发布：穷尽 37 变体 + 钉桩 + 差异/重叠两条防呆） |
| **A-2 第 2 步（2b）：B 改为再导出** | ✅ 2026-09-19（`wp-connector-utils` **0.3.4**；0.3.3 的依赖范围写法有误（待 yank）——公开路径不变，`wf-runtime` 零改动） |
| **A-2 第 3 步（2c）：值层迁入 C** | ✅ 2026-09-19（`wp-arrow 0.4.2` + `wp-connector-utils 0.3.5`；含两份逐字同构的金标准测试；C′ 的 `convert.rs` **故意未动**） |
| A ↔ C′ 参考对拍 | ✅ 保留（`arrow_contract_matches_wp_arrow_on_overlap`） |
| DIV-2 `BigInt` | ✅ 结论：零生产者，本轮不修（§4） |
| DIV-3 结构化元数据 | ✅ 结论：容忍是有意设计，本轮不修（§4） |

## 相关文件

| 文件 | 说明 |
|---|---|
| `wp-reactor/crates/wf-runtime/src/receiver/schema.rs` | A 期望侧映射 + 兼容性判定 + 钉桩/对拍测试 |
| `wp-reactor/crates/wf-runtime/src/receiver/route.rs` | `coerce_column` 的源类型白名单与**带 WARN 的目标类型 null** 兜底（2026-09-19 起不再返回 `NullArray`） |
| `crates-wp/wp-connector-utils/src/arrow/schema.rs` | B sink 侧映射（**再导出 C 的契约表**）+ 消费侧冒烟测试 |
| `crates-wp/wp-connector-utils/src/arrow/record.rs` | B 值层（**转发 C 的 `contract::value`**）+ 消费侧金标准测试 |
| `wfusion/wp-arrow/src/contract/mod.rs` | C **契约列类型表**（穷尽 37 变体） + 钉桩/防呆测试 |
| `wfusion/wp-arrow/src/contract/value.rs` | C **契约值层**（`encode_record` / `encode_records`；自 `wp-connector-utils` 逐字移植 + 金标准测试） |
| `wfusion/wp-arrow/src/schema.rs` | C `WpDataType`（9 变体）→ Arrow；**类型化前端**，不是契约 |
| `wfusion/wp-arrow/src/convert.rs` | C 值/列转换（`records_to_batch` / `batch_to_records`，当前 0 调用）；A-2 与 B 的 `record.rs` 合并在这一层 |
| `crates-wp/wp-model-core/src/model/types/meta.rs:79` | `DataType` 的 37 个变体定义（规格表的行） |
