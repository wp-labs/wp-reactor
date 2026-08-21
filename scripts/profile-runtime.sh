#!/bin/bash
# 真实运行时热路径统计——instrument-coverage 插桩 + 真实跑批（PGO 思路）
#
# 微基准（close_bench 等）只覆盖引擎内部路径；真实运行时（TCP 接收 / parse /
# 列式转换 / routing / 窗口 / emit / ack 全链路）的热点分布完全不同（实测：
# arrow-buffer 值读取与 foldhash 哈希是真实运行独有热点，微基准看不到）。
# 本脚本用 LLVM instrument-coverage 插桩构建 wfusion，跑真实 bench，精确统计
# 每行执行次数（比 macOS sample 更精确、比微基准更真实）。
#
# 用法：
#   RUNTIME_BENCH="q1 q4 q12" TOTAL=10m scripts/profile-runtime.sh
#   # 可选：RUNTIME_FEED=replay|stream、RUNTIME_RULES="q1 q4 q12"（bench.sh 透传）
#
# 产物：target/profile-runtime/{cov-build/,cov-pgo/*.profraw,runtime.profdata,report.txt}
# 报告：文件级覆盖 + 热点行 top N（真实运行执行计数）

set -e
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd -P)"

BENCH="${RUNTIME_BENCH:-q1 q4 q12}"
TOTAL="${TOTAL:-10m}"
FEED="${RUNTIME_FEED:-replay}"
OUT=$REPO_ROOT/target/profile-runtime
COV_BUILD=$OUT/cov-build
PGO=$OUT/cov-pgo
mkdir -p "$PGO"

echo "== 1/4 instrument-coverage 插桩构建（隔离 target: $COV_BUILD）"
# wfusion/wfgen 在 warp-fusion workspace（wp-reactor 的兄弟目录）；CARGO_TARGET_DIR 隔离
# shellcheck disable=SC2086
(cd ../warp-fusion && RUSTFLAGS="-Cinstrument-coverage -Cdebuginfo=1" \
  CARGO_TARGET_DIR="$(pwd -P)/$OUT/cov-build" \
  cargo build --release -p wfusion -p wfgen 2>&1 | tail -1)

BIN=$COV_BUILD/release
rm -f "$PGO"/*.profraw

echo "== 2/4 真实运行（LLVM_PROFILE_FILE → $PGO，bench.sh 透传）"
NEXMARK=../wf-examples/performance/nexmark_pk
for q in $BENCH; do
  echo "  -- $q $FEED $TOTAL"
  (cd "$NEXMARK" && WFUSION="$BIN/wfusion" WFGEN="$BIN/wfgen" \
    LLVM_PROFILE_FILE="$PGO/run_%p.profraw" ./bench.sh "$q" "$FEED" "$TOTAL") 2>&1 \
    | grep -E "^$q/replay|^$q/stream" || true
done

echo "== 3/4 合并 profraw"
LLVM_TOOLS="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/bin"
"$LLVM_TOOLS/llvm-profdata" merge -o "$OUT/runtime.profdata" "$PGO"/*.profraw

echo "== 4/4 分析报告"
"$LLVM_TOOLS/llvm-cov" export --instr-profile="$OUT/runtime.profdata" \
  --object "$BIN/wfusion" > "$OUT/runtime.json" 2>/dev/null
python3 - "$OUT/runtime.json" <<'PYEOF'
import json, sys

d = json.load(open(sys.argv[1]))
files = d["data"][0]["files"]

def is_src(fn):
    return "/wp-reactor/" in fn and "/src/" in fn and "tests" not in fn

src_files = [f for f in files if is_src(f["filename"])]

# 文件级覆盖汇总
cov = []
for f in src_files:
    fn = f["filename"].split("/wp-reactor/")[-1]
    total = hit = 0
    hot = []
    for seg in f.get("segments", []):
        line, count = seg[0], seg[2]
        if line <= 0:
            continue
        total += 1
        if count > 0:
            hit += 1
            hot.append((count, line))
    if total:
        cov.append((fn, total, hit, 100.0 * hit / total, hot))

# 热点行（全部 src 文件；同一文件行可能有多个 region，按最大计数去重）
hots_map = {}
for f in src_files:
    fn = f["filename"].split("/wp-reactor/")[-1]
    for seg in f.get("segments", []):
        if seg[0] > 0 and seg[2] > 0:
            key = (fn, seg[0])
            hots_map[key] = max(hots_map.get(key, 0), seg[2])
hots = [(count, line, fn) for (fn, line), count in hots_map.items()]
hots.sort(reverse=True)

lines = []
lines.append("=" * 100)
lines.append("真实运行时热路径报告（instrument-coverage 插桩 + 真实跑批）")
lines.append("=" * 100)
lines.append("")
lines.append(f"── 热点行 top 25（真实执行计数；每事件次数 = 计数 ÷ 事件总量）──")
for count, line, fn in hots[:25]:
    lines.append(f"  {fn}:{line:<6} 执行 {count:>14,} 次")

lines.append("")
lines.append(f"── 文件级覆盖（真实运行，共 {len(cov)} 个 src 文件）──")
lines.append(f"  {'文件':<66} {'行':>6} {'覆盖':>6} {'%':>6}")
for fn, total, hit, pct, _ in sorted(cov, key=lambda c: (-c[2], c[3]))[:20]:
    lines.append(f"  {fn:<66} {total:>6} {hit:>6} {pct:>5.1f}%")

report = "\n".join(lines)
with open("target/profile-runtime/report.txt", "w") as f:
    f.write(report + "\n")
print(report)
PYEOF
