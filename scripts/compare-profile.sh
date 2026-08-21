#!/bin/bash
# 覆盖差集对比——真实运行覆盖 vs 微基准覆盖
#
# 真实运行（PROFILE=1 ./bench.sh 产物：data/profile/runtime.json）执行了
# 微基准（scripts/profile-cov.sh 产物：target/profile-cov/bench.json）没覆盖的
# 行 = 全链路路径（TCP/parse/route/窗口/emit）——这些就是**需要补 test bench**
# （引擎级/集成基准）的地方；反之微基准独有的路径 = 引擎内部但真实跑批未触发
# （可能是 query 集合未覆盖的规则类型）。
#
# 用法：
#   scripts/compare-profile.sh                                  # 默认两份产物
#   RUNTIME_JSON=<path> BENCH_JSON=<path> scripts/compare-profile.sh
#
# 输出：target/profile-cov/coverage-diff.txt

set -e
cd "$(dirname "$0")/.."
ROOT="$(pwd -P)"

RUNTIME_JSON="${RUNTIME_JSON:-$ROOT/../wf-examples/performance/nexmark_pk/data/profile/runtime.json}"
BENCH_JSON="${BENCH_JSON:-$ROOT/target/profile-cov/bench.json}"
OUT=target/profile-cov
mkdir -p "$OUT"

python3 - "$RUNTIME_JSON" "$BENCH_JSON" "$OUT" <<'PYEOF'
import json, sys

runtime_path, bench_path, out_dir = sys.argv[1:4]

def load(path):
    with open(path) as f:
        return json.load(f)

def file_rows(data):
    """file -> {line: max_count}"""
    res = {}
    for f in data["data"][0]["files"]:
        fn = f["filename"]
        counts = {}
        for seg in f.get("segments", []):
            line, count = seg[0], seg[2]
            if count > 0 and line > 0:
                counts[line] = max(counts.get(line, 0), count)
        res[fn] = counts
    return res

def is_src(fn):
    return "/wp-reactor/" in fn and "/src/" in fn and "tests" not in fn

runtime = file_rows(load(runtime_path))
bench = file_rows(load(bench_path))

src = sorted(f for f in set(runtime) | set(bench) if is_src(f))

lines = []
lines.append("=" * 100)
lines.append("覆盖差集：真实运行（PROFILE bench） vs 微基准（profile-cov bench）")
lines.append(f"  真实运行: {runtime_path}")
lines.append(f"  微基准:   {bench_path}")
lines.append("=" * 100)

# A. 真实运行执行但微基准未覆盖 → 需补 test bench
lines.append("")
lines.append("── A. 需补 test bench（真实运行执行、微基准未覆盖的行）──")
rows_a = []
for fn in src:
    r, b = runtime.get(fn, {}), bench.get(fn, {})
    only_runtime = set(r) - set(b)
    if only_runtime:
        rows_a.append((fn, len(only_runtime), sorted(only_runtime)[:5]))
for fn, n, sample_lines in sorted(rows_a, key=lambda x: -x[1]):
    fn_short = fn.split("/wp-reactor/")[-1]
    lines.append(f"  {fn_short:<68} 缺微基准 {n:>5} 行（例: {sample_lines}）")

# B. 微基准执行但真实运行未覆盖 → 引擎内部路径未在真实跑批触发
lines.append("")
lines.append("── B. 微基准执行、真实运行未覆盖（引擎内部但本次 query 集未触发）──")
rows_b = []
for fn in src:
    r, b = runtime.get(fn, {}), bench.get(fn, {})
    only_bench = set(b) - set(r)
    if only_bench:
        rows_b.append((fn, len(only_bench)))
for fn, n in sorted(rows_b, key=lambda x: -x[1])[:15]:
    fn_short = fn.split("/wp-reactor/")[-1]
    lines.append(f"  {fn_short:<68} 真实未触发 {n:>5} 行")

# 汇总
total_a = sum(n for _, n, _ in rows_a)
total_b = sum(n for _, n in rows_b)
lines.append("")
lines.append(f"── 汇总：A（补 test bench）{total_a} 行 / B（真实未触发）{total_b} 行 ──")

report = "\n".join(lines)
with open(f"{out_dir}/coverage-diff.txt", "w") as f:
    f.write(report + "\n")
print(report)
PYEOF
