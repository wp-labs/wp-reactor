# file_input 示例

这个示例演示 `wfusion` 的 `file` 输入源：启动后自动读取 `data/port_scan.ndjson`，并把数据按 `stream_tag=netflow` 注入运行时。

## 文件说明

- `wfusion.toml`：`mode = "batch"`，启用 `[[sources]] type = "file"`，并复用 `../distinct` 的 schema/rule。
- `windows.toml`：窗口默认值与按窗口覆盖（由 `windows = "windows.toml"` 引用）。
- `data/port_scan.ndjson`：10 条 `syn` 事件，触发 `port_scan` 规则 1 次告警。

## 验证步骤

在仓库根目录执行：

```bash
rm -f examples/file_input/alerts/all.jsonl

cargo run --manifest-path ../warp-fusion/Cargo.toml --bin wfusion -- batch \
    --config examples/file_input/wfusion.toml

wc -l examples/file_input/alerts/all.jsonl
cat examples/file_input/alerts/all.jsonl
```

`batch` 模式回放完 `data/port_scan.ndjson` 后自动退出，无需手动发信号。

预期：

- `examples/file_input/alerts/all.jsonl` 存在；
- 行数为 `1`；
- `rule_name` 为 `port_scan`，`entity_id` 为 `10.0.0.1`。
