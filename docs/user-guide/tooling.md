# 开发与测试工具

`wfusion`、`wfgen`、`wfl` 三个 CLI 现在由相邻仓库 `../warp-fusion` 产出；`wp-reactor` 继续承载核心库、示例和文档。

## `wfadm conf`

配置诊断能力已从旧版 `wfusion config` 迁移到 `wfadm conf`（`wfadm` 由同级 `../warp-fusion` 仓库产出），用于排查：

- overlay merge 后到底是什么
- `${CASE_PATH}` / `${WORK_DIR}` 一类变量最终展开成什么
- 某个字段到底来自 base 还是 overlay
- 两组参数下配置哪里发生了变化

当前提供 `conf diff` 与 `conf update` 两个子命令。

### `wfadm conf diff`

比较两组加载参数下的配置差异：

```bash
wfadm conf diff \
    --config conf/wfusion.toml \
    --overlay conf/dev.toml \
    --to-overlay conf/batch.toml
```

左侧加载参数：`--config` / `--overlay` / `--var` / `--work-dir`；右侧参数：`--to-config` / `--to-overlay` / `--to-var` / `--to-work-dir`。

也可以比较不同变量输入：

```bash
wfadm conf diff \
    --config conf/wfusion.toml \
    --var CASE_PATH=/tmp/case-a \
    --to-var CASE_PATH=/tmp/case-b
```

比较“变量展开后的最终配置差异”，加 `--expanded`：

```bash
wfadm conf diff \
    --config conf/wfusion.toml \
    --var CASE_PATH=/tmp/case-a \
    --to-var CASE_PATH=/tmp/case-b \
    --expanded
```

`--expanded` 模式下，差异项的 `old_origin` / `new_origin` 会尽量显示“最终展开值来自哪里”：

- 纯文件来源时仍显示文件路径
- 变量驱动时显示 `<cli:CASE_PATH>` / `<env:HOME>` / `<builtin:WORK_DIR>` / `<default:FOO>`
- 如果一个最终值同时由多个来源拼接而成，则显示 `<mixed:...>`

`--path-prefix` 只看某个子树：

```bash
wfadm conf diff \
    --config conf/wfusion.toml \
    --to-overlay conf/batch.toml \
    --path-prefix runtime
```

> 旧版 `wfusion config render / origins / vars` 子命令已移除。变量解析顺序与 scoped var 语义见 [运行时配置](./runtime-config.md)。

## `wfl`

`wfl` 是 WFL 语言的开发者命令行工具，用于规则解释、检查、格式化、离线回放和契约测试。

### 子命令

| 子命令 | 用途 |
|--------|------|
| `explain` | 输出执行计划解释 |
| `lint` | 语义检查和 lint |
| `fmt` | 规则格式化 |
| `replay` | 用 NDJSON 离线回放 |
| `test` | 运行规则契约测试 |
| `verify` | 一步完成 replay + verify |

公共参数：

- `--schemas` / `-s`
- `--var KEY=VALUE`

### `wfl explain`

```bash
wfl explain rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --var FAIL_THRESHOLD=3
```

### `wfl lint`

```bash
wfl lint rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --var FAIL_THRESHOLD=3
```

### `wfl fmt`

```bash
wfl fmt rules/brute_force.wfl
wfl fmt -w rules/*.wfl
wfl fmt --check rules/*.wfl
```

### `wfl replay`

```bash
wfl replay rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --input test_data/events.jsonl \
    --var FAIL_THRESHOLD=3
```

限制：

- 离线模式无 window store
- `join` 查找和 `window.has()` guard 返回空值
- EOF 时自动触发 `close_all(Eos)`

### `wfl test`

```bash
wfl test rules/brute_force.wfl --schemas "schemas/*.wfs"
```

### `wfl verify`

推荐写法：

```bash
wfl verify --case brute_force --data-dir data
```

也可手工指定：

```bash
wfl verify rules/brute_force.wfl \
    --schemas "schemas/*.wfs" \
    --input data/brute_force.jsonl \
    --expected data/brute_force.except.jsonl \
    --meta data/brute_force.except.meta.jsonl \
    --format markdown
```

## `wfgen`

`wfgen` 用于生成测试数据，输入是 `.wfg` 场景文件。

### 场景示例

```wfg
use "schemas/security.wfs"
use "rules/brute_force.wfl"

#[duration=30m]
scenario brute_force_detect<seed=42> {
  traffic {
    stream auth_events gen 200/s
  }

  injection {
    hit<30%> auth_events {
      sip seq {
        use(action="failed") with(3)
      }
    }
  }

  expect {
    hit(brute_force_then_scan) >= 95%
  }
}
```

### CLI

生成并发送：

```bash
wfgen gen \
    --scenario examples/count/scenarios/brute_force.wfg \
    --format jsonl \
    --out out/ \
    --send \
    --addr 127.0.0.1:9800
```

一致性校验：

```bash
wfgen lint examples/count/scenarios/brute_force.wfg
```

复用已有数据单独发送：

```bash
wfgen send \
    --scenario examples/count/scenarios/brute_force.wfg \
    --input out/brute_force.jsonl \
    --addr 127.0.0.1:9800
```

对拍验证：

```bash
wfgen verify \
    --actual out/actual_alerts.jsonl \
    --expected out/brute_force.except.jsonl \
    --meta out/brute_force.except.meta.jsonl
```

持续压测：

```bash
wfgen bench \
    --scenario examples/count/scenarios/brute_force.wfg \
    --duration 5m \
    --send \
    --addr 127.0.0.1:9800
```

## 联合验证

推荐直接运行仓库内置 e2e（在 `../warp-fusion` 仓库中）：

```bash
cargo test -p wfgen e2e_datagen_brute_force -- --nocapture
```

该用例会自动完成：

1. 生成事件
2. 启动 `wfusion`
3. 通过 TCP 发送 Arrow IPC 数据
4. 将实际告警与 expected 对拍

手工分步调试：

```bash
wfusion batch \
    --config conf/wfusion.toml \
    --overlay conf/batch.toml \
    --work-dir /path/to/project \
    --metrics \
    --metrics-interval 2s
```

```bash
wfgen gen \
    --scenario examples/count/scenarios/brute_force.wfg \
    --format jsonl \
    --out out/ \
    --send \
    --addr 127.0.0.1:9800
```

```bash
wfl verify --case brute_force --data-dir out --format markdown
```
