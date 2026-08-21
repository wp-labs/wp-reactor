#!/bin/bash
# 性能 profile 覆盖率——测试覆盖 vs 性能基准覆盖 双维度统计
#
# 动机：代码里的大量低效实现（如 push_capped 的 O(n²) Vec::drain）靠 sample
# 碰运气才能发现。本脚本把「覆盖率」与「性能路径」结合，系统化暴露三类问题：
#
#   A. 死/冷代码    —— 测试与性能基准都未覆盖的行（低效代码可能藏在未验证路径）
#   B. 热路径缺测试 —— 性能基准执行但测试未覆盖的行（热路径无正确性保护，
#                      改坏时基准跑通、单测抓不到）
#   C. 热点行       —— 性能基准中执行计数最高的行（优化候选；配合 macOS
#                      sample 的耗时归因定位低效实现）
#
# 用法：
#   scripts/profile-cov.sh [--bench "close_bench each_bench ..."] [--packages "wf-engine wf-runtime"]
#   默认：--bench "close_bench each_bench guard_bench match_bench" --packages 全 workspace
#
# 输出：target/profile-cov/{tests.json,bench.json,report.txt}

set -e
cd "$(dirname "$0")/.."

BENCHES="${BENCHES:-close_bench each_bench guard_bench match_bench}"
PKGS="${PKGS:-}"
OUT=target/profile-cov
mkdir -p "$OUT"

PKG_ARGS=""
[ -n "$PKGS" ] && for p in $PKGS; do PKG_ARGS="$PKG_ARGS -p $p"; done

echo "== 1/3 测试覆盖率（--tests）"
# 清理上次运行的 profraw（llvm-cov 实际工作目录 target/llvm-cov-target/），
# 避免两次运行的计数混叠（否则 bench.json 会包含 tests 的覆盖，差集失真）
rm -f target/llvm-cov-target/*.profraw 2>/dev/null || true
# shellcheck disable=SC2086
cargo llvm-cov $PKG_ARGS --no-clean --tests --json > "$OUT/tests.json" 2>/dev/null

# 基准运行前再次清理（基准的计数须独立于测试）
rm -f target/llvm-cov-target/*.profraw 2>/dev/null || true

echo "== 2/3 性能基准覆盖率（--ignored: $BENCHES）"
# shellcheck disable=SC2086
cargo llvm-cov $PKG_ARGS --no-clean --tests --json -- --ignored $BENCHES > "$OUT/bench.json" 2>/dev/null

echo "== 3/3 对比分析"
python3 - "$OUT" "$BENCHES" <<'PYEOF'
import json, os, sys

out_dir, benches = sys.argv[1], sys.argv[2].split()

def load(name):
    with open(os.path.join(out_dir, name)) as f:
        return json.load(f)

tests, bench = load("tests.json"), load("bench.json")

def file_rows(data):
    """file -> {line: max_count}"""
    res = {}
    for f in data["data"][0]["files"]:
        fn = f["filename"]
        counts = {}
        for seg in f.get("segments", []):
            line, _, count = seg[0], seg[1], seg[2]
            if count > 0 and line > 0:
                counts[line] = max(counts.get(line, 0), count)
        res[fn] = counts
    return res

tr, br = file_rows(tests), file_rows(bench)

# 只统计 src/ 生产代码（排除 tests/ 文件本身与 target 内路径）
def is_src(fn):
    return "/src/" in fn and not fn.endswith("/tests.rs") and "/tests/" not in fn

src_files = sorted(set(tr) | set(br))
src_files = [f for f in src_files if is_src(f)]

rows = []
for fn in src_files:
    t, b = tr.get(fn, {}), br.get(fn, {})
    all_lines = set(t) | set(b)
    only_test = set(t) - set(b)
    only_bench = set(b) - set(t)
    neither = all_lines - set(t) - set(b)
    hot = sorted(b.items(), key=lambda kv: -kv[1])[:5]
    rows.append({
        "file": fn,
        "lines": len(all_lines),
        "test_only": len(only_test),
        "bench_only": len(only_bench),
        "neither": len(neither),
        "hot": hot,
    })

def fmt(fn):
    return fn.split("/wp-reactor/")[-1]

lines_out = []
lines_out.append("=" * 100)
lines_out.append("性能 profile 覆盖率报告（测试覆盖 vs 性能基准覆盖）")
lines_out.append(f"基准: {', '.join(benches)}")
lines_out.append("=" * 100)

lines_out.append("")
lines_out.append("── A. 死/冷代码候选（测试与基准都未覆盖的行最多，可能藏低效实现）──")
for r in sorted(rows, key=lambda r: -r["neither"])[:15]:
    if r["neither"] > 0:
        lines_out.append(f"  {fmt(r['file']):<70} 未覆盖 {r['neither']:>5} / {r['lines']:>5} 行")

lines_out.append("")
lines_out.append("── B. 热路径缺测试（基准执行但测试未覆盖——改坏时单测抓不到）──")
for r in sorted(rows, key=lambda r: -r["bench_only"])[:15]:
    if r["bench_only"] > 0:
        lines_out.append(f"  {fmt(r['file']):<70} 缺测试 {r['bench_only']:>5} 行")

lines_out.append("")
lines_out.append("── C. 热点行（性能基准执行计数 top，每文件前 5 行）──")
shown = 0
for r in sorted(rows, key=lambda r: -(r["hot"][0][1] if r["hot"] else 0))[:20]:
    for line, count in r["hot"]:
        if shown >= 25:
            break
        lines_out.append(f"  {fmt(r['file'])}:{line:<6} 执行 {count:>12,} 次")
        shown += 1
    if shown >= 25:
        break

# 汇总表（文件级）
lines_out.append("")
lines_out.append("── 汇总（文件级：总行 / 仅测试 / 仅基准 / 双未覆盖）──")
hdr = f"  {'文件':<70} {'行':>6} {'测-only':>7} {'基-only':>7} {'双无':>6}"
lines_out.append(hdr)
for r in sorted(rows, key=lambda r: -r["lines"])[:25]:
    lines_out.append(
        f"  {fmt(r['file']):<70} {r['lines']:>6} {r['test_only']:>7} {r['bench_only']:>7} {r['neither']:>6}"
    )

report = "\n".join(lines_out)
with open(os.path.join(out_dir, "report.txt"), "w") as f:
    f.write(report + "\n")
print(report)
PYEOF
