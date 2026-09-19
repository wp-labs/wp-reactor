# wp-model ↔ Arrow 类型映射规格表（三方口径）

> **状态：现行口径快照（2026-09）**
>
> 本文是 **wparse（sink 侧）↔ wfusion（接收侧）** 之间 Arrow 列类型契约的**单一事实来源**。
> 三处手工映射表的**唯一权威版本**就是下面的规格表；任何一处改动都必须同时更新本文档与对应的钉桩测试。
>
> 姊妹文档：[`arrow-tcp-stream-compatibility.md`](./arrow-tcp-stream-compatibility.md)（IPC 帧/framing 口径，已实现）。

## 1. 为什么需要这份表

同一份 `wp_model_core::model::DataType` 目前被**三处互不可见的手工映射表**翻译成 Arrow 类型：

| 角色 | 位置 | 输入类型 | 覆盖 |
|---|---|---|---|
| **A. 期望侧（线协议契约）** | `crates/wf-runtime/src/receiver/schema.rs:127` `field_type_to_arrow` / `:142` `base_type_to_arrow` | `wf_lang::FieldType`（WPL 窗口声明） | 7 个 `BaseType` + `Object` / `ArrayAny` / `Array(BaseType)` |
| **B. sink 侧** | `crates-wp/wp-connector-utils/src/arrow/schema.rs:27` `wp_type_to_arrow`（私有，经 `:73` `infer_schema_from_record` 生效） | `wp_model_core::model::DataType` | **穷尽 37 个变体**（新增变体会编译失败） |
| **C. 映射 crate** | `wfusion/wp-arrow/src/schema.rs:50` `to_arrow_type` / `:95` `parse_wp_type` | `wp_arrow::schema::WpDataType` | 9 个变体（未覆盖即 `UnsupportedDataType` 运行时错误） |

三者是**同一语义的三份拷贝**，没有任何编译期或测试期的相互约束 —— 这是「口径静默分叉」的结构性根因。
本文把差异**显式登记**，并用钉桩测试把「当前行为」钉死，使漂移必然触发测试失败而不是线上静默错误。

### 可链接的对拍范围

A 与 C 都位于 `wfusion` 仓，且 `wf-runtime` 已依赖 crates.io 的 `wp-arrow`（当前解析 `0.3.1`），
**因此 A ↔ C 可以在同一测试二进制内真实对拍**（见 `receiver/schema.rs` 的 `arrow_contract_matches_wp_arrow_on_overlap`）。
B 位于 `crates-wp` 仓、且其映射函数为私有，无法与 A 链接 —— 对 B 的约束方式是
「B 的钉桩测试 + 本文档的同一张表」（`wp-connector-utils/src/arrow/schema.rs` 的 `arrow_contract_full_mapping_is_pinned`）。

> 注：crates.io 的 `wp-arrow 0.3.1` 与本地 `wfusion/wp-arrow` 的 `schema.rs` 映射逐字相同，故 A↔C 对拍对两者都成立。
>
> 对拍时两侧各自把 Arrow `DataType` `Debug` 成字符串再比较，而不是直接 `assert_eq!`：
> `wp-arrow 0.3.1` 依赖 `arrow 59`，而 `wp-reactor` 的 `arrow 60` 升级在途，两边类型不同就根本编不过。

> **不要动 `WpDataType::Digit` 这个名字**：它是 `wp-arrow` 自己的 9 变体枚举，不是 wp-model-core 的 `DataType`。
> 家族里那轮 `Digit → Int` 正名**刻意没有触及**它（`schema.rs:10`、`schema.rs:101` 的字面串 `"digit"`），
> 改它会同时改字段元数据字符串与线上行为。两侧的对应关系只在 `convert.rs` 的 `wp_type_to_model_meta` 里显式建立。

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

图例：`n/a` = 该侧的表达能力**无法表示**此类型；`⚠` = 与其它侧冲突（见 §4）。

| # | DataType | serde | A 期望侧（WPL） | B wp-connector-utils | C wp-arrow |
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

| 编号 | 类型 | 分歧 | 严重度 | 后果 |
|---|---|---|---|---|
| **DIV-2** | `BigInt` | B = `Utf8`（十进制字符串），C = `Decimal256(39,0)`（数值） | P1 · 语义分叉 | 同一字段两种传输语义；A 无此类型故线上不冲突，但数值语义只能靠 C 保留 |
| **DIV-3** | `Obj` / `Array` | B 给 `Utf8` 且**不带** `wfl_field_type` 元数据；A 给 `Utf8`+meta；C 给 `List(inner)` | P2 · 可容忍 | 接收侧对「`Utf8` 无 meta」返回兼容（`schema.rs:60`），故**能过**；代价是 object/array 语义无法据此校验，静默接受 |
| **DIV-4** | 覆盖范围 | 7 / 37 / 9 个变体；C 未覆盖是**运行时**报错 | P1 · 结构性 | 三张手工表必然继续漂移；是「统一实现」（任务 A）的动因，但不是任何单个 P0 的成因 |

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

1. 改代码（A / B / C 中受影响的那一处）。
2. 更新本文档 §3 对应行、§4 的差异表（差异消失就删除该行，不要留「历史差异」）。
3. 更新钉桩测试：
   - A：`crates/wf-runtime/src/receiver/schema.rs` → `arrow_contract_*`
   - B：`crates-wp/wp-connector-utils/src/arrow/schema.rs` → `arrow_contract_full_mapping_is_pinned`
   - C：`wfusion/wp-arrow/src/schema.rs` → 现有 `arrow_type_*` 全变体测试
4. 「差异存在」这件事本身由测试表达：能够容忍的差异写 `assert_ne!` 并注明 DIV 编号；
   不能容忍的差异写「当前会失败」的特征测试（characterization test）并注明修正方向。

## 6. 现状与后续

| 项 | 状态 |
|---|---|
| 输出本文档（三方规格表 + 已知差异登记） | ✅ 本次 |
| A ↔ C 真对拍（同二进制） | ✅ 本次（`receiver/schema.rs`） |
| B 全 37 变体钉桩 | ✅ 本次（`wp-connector-utils/src/arrow/schema.rs`） |
| 修 DIV-1（P0，`Hex`） | ✅ 已修复并生效（2026-09-19；`wp-connector-utils` 0.3.1 已发布，本仓 lock 已跟进） |
| 消 P0-1（IPC 多批次帧只读第 1 批） | ✅ 已修复（2026-09-19，`wf-runtime` `decode_ipc_trusted` 全量解出） |
| 合并三份表为单一实现 | ⏳ 待定（任务 A；依赖跨仓发布顺序） |

## 相关文件

| 文件 | 说明 |
|---|---|
| `wp-reactor/crates/wf-runtime/src/receiver/schema.rs` | A 期望侧映射 + 兼容性判定 + 钉桩/对拍测试 |
| `wp-reactor/crates/wf-runtime/src/receiver/route.rs` | `coerce_column` 的源类型白名单与 null 兜底 |
| `crates-wp/wp-connector-utils/src/arrow/schema.rs` | B sink 侧映射 + 全量钉桩测试 |
| `crates-wp/wp-connector-utils/src/arrow/record.rs` | B 值层（值→列；`Hex` 现为 `Utf8` 列，值层对拍测试在此） |
| `wfusion/wp-arrow/src/schema.rs` | C `WpDataType` → Arrow |
| `crates-wp/wp-model-core/src/model/types/meta.rs:79` | `DataType` 的 37 个变体定义（规格表的行） |
