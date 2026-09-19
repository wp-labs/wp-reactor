# wp-model ↔ Arrow 类型映射规格表（三方口径）

> **状态：现行口径快照（2026-09）**
>
> 本文是 **wparse（sink 侧）↔ wfusion（接收侧）** 之间 Arrow 列类型契约的**单一事实来源**。
> 三处手工映射表的**唯一权威版本**就是下面的规格表；任何一处改动都必须同时更新本文档与对应的钉桩测试。
>
> 姊妹文档：[`arrow-tcp-stream-compatibility.md`](./arrow-tcp-stream-compatibility.md)（IPC 帧/framing 口径，已实现）。

## 1. 现状：两张活表 + 一张非生产表

同一份 `wp_model_core::model::DataType` 目前被三处手工映射表翻译成 Arrow 类型，但**只有两处在生产路径上**：

| 角色 | 位置 | 输入类型 | 覆盖 | 在线上吗 |
|---|---|---|---|---|
| **A. 期望侧（线协议契约）** | `crates/wf-runtime/src/receiver/schema.rs` `field_type_to_arrow` | `wf_lang::FieldType`（WPL 窗口声明） | 7 个 `BaseType` + `Object` / `ArrayAny` / `Array(BaseType)` | ✅ |
| **B. sink 侧（生产者）** | `crates-wp/wp-connector-utils/src/arrow/schema.rs` `wp_type_to_arrow` | `wp_model_core::model::DataType` | **穷尽 37 个变体**（新增变体会编译失败） | ✅ |
| **C. `wp-arrow` 的映射 API** | `wfusion/wp-arrow/src/schema.rs` `to_arrow_type` / `parse_wp_type` | `wp_arrow::schema::WpDataType` | 9 个变体 | ❌ **不在线协议路径上** |

### C 的定性（A-0，2026-09-19 实测）

`wp_arrow` 在生产代码里**只有 `ipc` 模块被使用**（全家族 30 处：`encode_ipc` / `decode_ipc` / `encode_ipc_frame_multi` 等）；
`schema` 的使用者是**本仓的契约测试自己**（1 处），`convert`（含 `wp_type_to_model_meta`）是 **0 处**。

也就是说 `to_arrow_type` / `parse_wp_type` 这套映射虽有完整单测，但**没有任何生产调用方**。
它保留为「强类型 API」（给外部/未来用），**不参与线协议契约** —— 所以下文的对拍与守卫只把 **A 与 B** 当作契约方，
涉及 C 的比较仅作参考（防止有人以为它在用；我自己一度就误判过）。

> `Ip` / `Hex` 等值在 C 里走 `Utf8`、`Array` 走 `List(inner)`、`BigInt` 走 `Decimal256`，都是**它自己的口径**，
> 与线协议无关。**不要动 `WpDataType::Digit` 这个名字**：它是 `wp-arrow` 自己的 9 变体枚举，
> 家族那轮 `Digit → Int` 正名刻意没触及它（改它会同时改字段元数据字符串与行为）。

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

> A↔C 的比较**保留为参考**（`arrow_contract_matches_wp_arrow_on_overlap`）：C 不在契约里（A-0），
> 但若将来真有人启用 `wp-arrow` 的映射 API，它能立刻报出口径差异。

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

**即使绕过校验，也不要指望被救回来**：`receiver/route.rs:122` `coerce_column` 只认 `Utf8` / `Int64` / `Float64` 作源类型，
其它一律 `:134` `_ => Arc::new(NullArray::new(num_rows))` —— 列会整体变 null（静默丢值）。

## 3. 规格表：`wp_model_core::model::DataType`（37 变体）× 三方

图例：`n/a` = 该侧的表达能力**无法表示**此类型。

> **A-1 之后（2026-09-19）这张表的读法变了**：
> **B 列是唯一实现**（`wp_connector_utils::arrow::wp_type_to_arrow`，穷尽 37 变体）；
> **A 列不再是自己一张表**，而是 `WPL 类型 → wp_model_core::DataType → B` 推导出来的 ——
> 所以 A 与 B 在 `Base` 与结构化行上**必然一致**（值保留在此仅供核对）。
> **C 列不参与契约**（§1 的 A-0 定性），列在这里只为对照 `wp-arrow` 的自有口径。

| # | DataType | serde | A 期望侧（WPL，由 B 推导） | B wp-connector-utils（唯一实现） | C wp-arrow（参考） |
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
即便绕过校验，`coerce_column` 的 `_ => NullArray`（`route.rs:134`）也会把整列变 null。

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

1. 改代码。**只改 B**（`wp-connector-utils/src/arrow/schema.rs`）—— A 会自动跟随（A-1 同源）。
   若确实需要动 A，那只能是改 **`WPL 类型 → wp_model_core::DataType` 的降级表**
   （`wf-runtime/src/receiver/schema.rs` 的 `base_type_to_model` / `field_type_to_model`）。
2. 更新本文档 §3 对应行、§4 的差异表（差异消失就删除该行，不要留「历史差异」）。
3. 更新钉桩测试：
   - B：`crates-wp/wp-connector-utils/src/arrow/schema.rs` → `arrow_contract_full_mapping_is_pinned`
   - A：`crates/wf-runtime/src/receiver/schema.rs` → `arrow_contract_expected_column_is_pinned`
     与 `arrow_contract_wpl_types_lower_to_model_types`
   - C（非契约）：`wfusion/wp-arrow/src/schema.rs` → 现有 `arrow_type_*` 全变体测试
4. **改 B 要过 wp-reactor 的 CI**：B 是已发布 crate，改它需要发 `0.3.x` patch，
   然后在本仓 `cargo update -p wp-connector-utils`（本仓 `Cargo.lock` 不跟踪，CI 自行解析）。
   跨仓回归靠 `arrow_contract_sink_inferred_hex_schema_passes_the_receiver`（走真实生产者路径 + 接收侧校验）。
5. 「差异存在」这件事本身由测试表达：能够容忍的差异写 `assert_ne!` 并注明 DIV 编号。

## 6. 现状与后续

| 项 | 状态 |
|---|---|
| 输出本文档（三方规格表 + 已知差异登记） | ✅ 2026-09-19 |
| B 全 37 变体钉桩 | ✅ `wp-connector-utils/src/arrow/schema.rs` |
| 修 DIV-1（P0，`Hex`） | ✅ 已修复并生效（`wp-connector-utils` 0.3.1 已发布） |
| 消 P0-1（IPC 多批次帧只读第 1 批） | ✅ 已修复（`wf-runtime` `decode_ipc_trusted` 全量解出） |
| **A-0：C 定性为「不在线协议路径上」** | ✅ 2026-09-19（§1；实测 `schema` 1 处使用者=本仓测试，`convert` 0 处） |
| **A-1：两张活表变一张** | ✅ 2026-09-19（`wp-connector-utils` 0.3.2 公开 `wp_type_to_arrow`；A 改为由 B 推导） |
| A ↔ C 参考对拍 | ✅ 保留（若将来有人真用 `wp-arrow` 的映射 API，立刻报差异） |
| DIV-2 `BigInt` | ✅ 结论：零生产者，本轮不修（§4） |
| DIV-3 结构化元数据 | ✅ 结论：容忍是有意设计，本轮不修（§4） |

## 相关文件

| 文件 | 说明 |
|---|---|
| `wp-reactor/crates/wf-runtime/src/receiver/schema.rs` | A 期望侧映射 + 兼容性判定 + 钉桩/对拍测试 |
| `wp-reactor/crates/wf-runtime/src/receiver/route.rs` | `coerce_column` 的源类型白名单与 null 兜底 |
| `crates-wp/wp-connector-utils/src/arrow/schema.rs` | B sink 侧映射 + 全量钉桩测试 |
| `crates-wp/wp-connector-utils/src/arrow/record.rs` | B 值层（值→列；`Hex` 现为 `Utf8` 列，值层对拍测试在此） |
| `wfusion/wp-arrow/src/schema.rs` | C `WpDataType` → Arrow |
| `crates-wp/wp-model-core/src/model/types/meta.rs:79` | `DataType` 的 37 个变体定义（规格表的行） |
