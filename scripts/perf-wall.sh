#!/usr/bin/env bash
# perf-wall.sh — 性能墙定位机制（PERF_BISECTION_METHOD 的机制化工具）
#
# 一段命令定位吞吐瓶颈在管线哪一段（注入/解码/窗口/规则/输出），全部刀用
# **声明式配置/env 切换**（不改引擎代码）：
#
#   段           刀                    切什么                             声明式开关
#   ───────────  ─────────────────────  ───────────────────────────────  ──────────────
#   ① 输出       cut=output            serialize+stage+commit+fanout     sinks 指向空目录
#   ② 规则       cut=rules            规则求值整段                       rules 指向空/子集文件
#   ③ 解码校验   已默认跳过（decode_ipc_trusted with_skip_validation）   —
#   ④ preread 预算 cut=budget:X        parse_buffer_bytes=X             config
#   ⑤ 计时开销   env=prof-off          WF_RULE_PROFILING=0              env
#   ⑥ 连接并行   --connections N       注入并行（C-UCP 供给）            env
#   ⑦ 磁盘供给   --dd-check            dd 读帧速 → 页面缓存             环境
#
# 测量协议（固化，防测量假象——2026-08-23 实证：metrics 1s 落盘把 300k floor
# 钉死在 ~1.06s 下限，26万 vs 修正后 970万）：
#   - 强制 report_interval=100ms + 50ms 轮询（metrics exporter 粒度 ≤ 轮询粒度）；
#   - 完成判定 = append_total 追平 N 且 acked_lag 归零（子集/空规则缺消费者时
#     用 append 停滞兜底），EPS = N / 全墙钟；
#   - 每档 2 轮取 max（降机器负载噪声），记录 load；
#   - 采样 daemon CPU%（忙墙 vs 等墙判别，方法论 §2.4）与 RSS 峰值。
#
# 用法:
#   scripts/perf-wall.sh <bench-dir> --frames <file> [选项]
#     选项:
#       --n N            事件数（默认 1000000）
#       --rounds R       每档轮数（默认 2，取 max）
#       --cuts LIST      刀列表，逗号分隔，默认 "floor,rules,sink,budget"
#                        可选: floor | rules | sink | budget:<bytes> | prof-off
#       --rules FILE     全量规则文件（默认 <bench>/models/rules/*.wfl 的 glob）
#       --rule-subset GLOB 规则名前缀过滤（grep -E 匹配名），与 rules 搭配
#       --conf FILE      基准 config（默认 <bench>/conf/wfusion.toml）
#       --sinks DIR      真实 sink 目录（默认 <bench>/topology/sinks）
#       --connections N  注入连接数（默认 1）
#       --rate-bytes B   send-arrow 限速（>0 = 持续注入，测绝对能力；0 = burst）
#       --skip-gen       帧文件已存在（默认：缺帧时报错退出，不自动生成）
#
# 输出：每档 EPS/CPU%/RSS/load + 增量成本表 + 墙判定。
#
# 依赖：warp-fusion 的 release wfusion/wfgen（REPO_ROOT 或 PATH 解析，同 run.sh）。

set -u
BENCH="${1:?用法: perf-wall.sh <bench-dir> --frames <file> ...}"
shift
FRAMES=""; N=1000000; ROUNDS=2; CUTS="floor,rules,sink,budget"
RULES=""; CONF=""; SINKS=""; SUBSET=""; CONNECTIONS=1; RATE_BYTES=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --frames) FRAMES="$2"; shift 2 ;;
    --n) N="$2"; shift 2 ;;
    --rounds) ROUNDS="$2"; shift 2 ;;
    --cuts) CUTS="$2"; shift 2 ;;
    --rules) RULES="$2"; shift 2 ;;
    --rule-subset) SUBSET="$2"; shift 2 ;;
    --conf) CONF="$2"; shift 2 ;;
    --sinks) SINKS="$2"; shift 2 ;;
    --connections) CONNECTIONS="$2"; shift 2 ;;
    --rate-bytes) RATE_BYTES="$2"; shift 2 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

[ -n "$FRAMES" ] || { echo "错误: --frames 必填（用 bench 的 run.sh/bench.sh 先产出帧文件）" >&2; exit 2; }
[ -f "$FRAMES" ] || { echo "错误: 帧文件不存在: $FRAMES" >&2; exit 2; }

# ---- 二进制解析（同 run.sh 逻辑）----
PROFILE="${PROFILE:-release}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WF=""; GEN=""
if [ -x "$REPO_ROOT/../warp-fusion/target/$PROFILE/wfusion" ]; then
  WF="$REPO_ROOT/../warp-fusion/target/$PROFILE/wfusion"
fi
if [ -x "$REPO_ROOT/../warp-fusion/target/$PROFILE/wfgen" ]; then
  GEN="$REPO_ROOT/../warp-fusion/target/$PROFILE/wfgen"
fi
[ -z "$WF" ] && WF="$(command -v wfusion 2>/dev/null || true)"
[ -z "$GEN" ] && GEN="$(command -v wfgen 2>/dev/null || true)"
if [ -z "$WF" ] || [ -z "$GEN" ]; then
  echo "错误: 找不到 wfusion/wfgen（构建 warp-fusion release 或加入 PATH）" >&2; exit 1
fi

PY=python3
CONF="${CONF:-$BENCH/conf/wfusion.toml}"
SINKS="${SINKS:-$BENCH/topology/sinks}"
RULES="${RULES:-$BENCH/models/rules}"
[ -f "$CONF" ] || { echo "错误: config 不存在: $CONF" >&2; exit 1; }

cd "$BENCH"
pkill -9 -f "wfusion daemon" 2>/dev/null; sleep 1
mkdir -p data; : > data/metrics.ndjson
TMP="$(mktemp -d /tmp/perfwall.XXXXXX)"
trap 'pkill -9 -f "wfusion daemon" 2>/dev/null; rm -rf "$TMP"' EXIT

# 全量规则（$RULES 是目录或文件），供 rules/子集档使用
if [ -d "$RULES" ]; then
  for f in "$RULES"/*.wfl; do cat "$f"; echo; done > "$TMP/rules_all.wfl"
elif [ -f "$RULES" ]; then
  cat "$RULES" > "$TMP/rules_all.wfl"
else
  echo "错误: 规则路径不存在（目录或文件）: $RULES" >&2; exit 1
fi
RULECOUNT=$(grep -c "^rule " "$TMP/rules_all.wfl")
echo "bench=$BENCH N=$N rounds=$ROUNDS cuts=$CUTS rules=$RULECOUNT frames=$FRAMES"
echo "  load=$(uptime | sed 's/.*averages:/averages:/')"

# ---- 配置模板化：产出某档的 config ----
# gen_conf <rules_file> <sinks_dir> <report_interval> <parse_buffer_bytes> <out>
gen_conf() {
  local RULEF="$1" SINKD="$2" RI="$3" PBB="$4" OUT="$5"
  sed -e "s|rules = .*|rules = \"$RULEF\"|" \
      -e "s|sinks = .*|sinks = \"$SINKD\"|" \
      -e "s|report_interval = .*|report_interval = \"$RI\"|" \
      -e "s|parse_buffer_bytes = .*|parse_buffer_bytes = $PBB|" \
      -e '/^max_ingest_rate/d' \
      "$CONF" > "$OUT"
}

# 空规则文件（管道净段 / 无规则档）
: > "$TMP/rules_empty.wfl"
# 空 sink 目录（切输出）
mkdir -p "$TMP/sinks_empty"

# ---- 单轮测量（100ms 协议 + 50ms 轮询 + append/lag 完成判定）----
measure_once() { # $1 = tag  $2 = conf  $3 = 额外 env（形如 "VAR=1 VAR2=0"）
  local TAG="$1" C="$2" ENVX="$3"
  : > data/metrics.ndjson
  if [ -n "$ENVX" ]; then
    # shellcheck disable=SC2086
    env $ENVX "$WF" daemon --config "$C" --work-dir . > /tmp/perfwall_${TAG}.log 2>&1 &
  else
    "$WF" daemon --config "$C" --work-dir . > /tmp/perfwall_${TAG}.log 2>&1 &
  fi
  local D=$!
  local READY=0
  for i in $(seq 1 40); do nc -z 127.0.0.1 9800 2>/dev/null && { READY=1; break; }; sleep 0.2; done
  if [ "$READY" != 1 ]; then echo "MEAS $TAG daemon-not-ready"; kill $D 2>/dev/null; pkill -9 -f "wfusion daemon" 2>/dev/null; return 1; fi
  local T0=$($PY -c 'import time;print(time.time())')
  # send-arrow：多连接走 --connections（单连接无键闭包问题；有状态多连接需
  # 调用方先 shard-frames + --shard-files，v1 默认单连接）
  local RATE_ARG=""; [ "$RATE_BYTES" -gt 0 ] && RATE_ARG="--rate-bytes $RATE_BYTES"
  "$GEN" send-arrow --input "$FRAMES" --addr 127.0.0.1:9800 --connections "$CONNECTIONS" $RATE_ARG >/dev/null 2>&1
  # CPU/RSS 采样循环（与消化轮询并行）
  local CPUS=""
  local END="" APP=0 LAG=9 PREV=-1 STALL=0 ZERO=0
  local I=0
  for i in $(seq 1 2400); do
    APP=$("$PY" - data/metrics.ndjson <<'PYEOF'
import json, sys
s = 0
for line in open(sys.argv[1], errors='replace'):
    try: o = json.loads(line)
    except Exception: continue
    if o.get('name') == 'append_total':
        s += int(o.get('value', 0))
print(s)
PYEOF
)
    LAG=$("$PY" - data/metrics.ndjson <<'PYEOF'
import json, sys
lag = {}
for line in open(sys.argv[1], errors='replace'):
    try: o = json.loads(line)
    except Exception: continue
    if o.get('name') == 'acked_lag':
        lag[o.get('label','?')] = int(o.get('value', 0))
print(sum(lag.values()))
PYEOF
)
    if [ "$APP" -eq 0 ]; then ZERO=$((ZERO+1)); else ZERO=0; fi
    if [ "$ZERO" -ge 40 ]; then break; fi
    if [ "$APP" = "$PREV" ] && [ "$APP" -gt 0 ]; then STALL=$((STALL+1)); else STALL=0; PREV=$APP; fi
    if { [ "${APP:-0}" -ge "$N" ] || [ "$STALL" -ge 6 ]; } && [ "${LAG:-9}" = "0" ]; then
      END=$($PY -c 'import time;print(time.time())'); break
    fi
    I=$(( I + 1 ))
    if [ $(( I % 4 )) -eq 0 ]; then
      CPUS="$CPUS $(ps -o %cpu= -p $D 2>/dev/null | tr -d ' ')"
    fi
    sleep 0.05
  done
  [ -n "$END" ] || { END=$($PY -c 'import time;print(time.time())'); }
  local TOT=$($PY -c "print($END-$T0)")
  local EPS=0; [ "$TOT" != "0" ] && EPS=$($PY -c "print(int($N/$TOT))")
  local CPUAVG=0; [ -n "$CPUS" ] && CPUAVG=$(echo $CPUS | tr ' ' '\n' | grep -E '^[0-9.]+$' | awk '{s+=$1;n++} END{if(n>0)printf "%.0f", s/n; else print 0}')
  local RSSKB=$(ps -o rss= -p $D 2>/dev/null | tr -d ' ')
  local RSSMB=0; [ -n "$RSSKB" ] && [ "$RSSKB" -gt 0 ] 2>/dev/null && RSSMB=$($PY -c "print(int($RSSKB/1024))")
  echo "MEAS $TAG eps=$EPS full=${TOT}s cpu=${CPUAVG}% rss=${RSSMB}MB append=$APP lag=$LAG"
  kill $D 2>/dev/null; pkill -9 -f "wfusion daemon" 2>/dev/null; sleep 1
  return 0
}

measure() { # $1 = tag  $2 = conf  $3 = env
  local TAG="$1" C="$2" E="$3"
  local BEST=0
  for r in $(seq 1 "$ROUNDS"); do
    local L EPS
    L=$(measure_once "${TAG}_r${r}" "$C" "$E")
    echo "  $L" | tee -a "$TMP/wall.txt"
    EPS=$(echo "$L" | sed 's/.*eps=\([0-9]*\).*/\1/')
    [ "$EPS" -gt "$BEST" ] 2>/dev/null && BEST=$EPS
  done
  echo "MEAS ${TAG}_best eps=$BEST" | tee -a "$TMP/wall.txt"
}

get_eps() { # 取最近一次 _best 行的 eps（供墙表外的即时读取）
  grep "_best eps=" "$TMP/wall.txt" | tail -1 | sed 's/.*eps=\([0-9]*\).*/\1/'
}

# ---- 墙梯：floor → rules → sink（叠加式，从尾部向前切）----
echo "== 墙梯（cut 列表: ${CUTS}）=="
PREV_EPS=0
declare -A EPS_MAP
for CUT in $(echo "$CUTS" | tr ',' ' '); do
  case "$CUT" in
    floor)
      gen_conf "$TMP/rules_empty.wfl" "$TMP/sinks_empty" "100ms" "2147483648" "$TMP/conf_floor.toml"
      measure floor "$TMP/conf_floor.toml" ""
      EPS_MAP[floor]=$(get_eps)
      echo "  → floor(管道净段) = ${EPS_MAP[floor]} EPS"
      ;;
    rules)
      gen_conf "$TMP/rules_all.wfl" "$TMP/sinks_empty" "100ms" "2147483648" "$TMP/conf_rules.toml"
      measure rules "$TMP/conf_rules.toml" ""
      EPS_MAP[rules]=$(get_eps)
      echo "  → rules(+全量${RULECOUNT}条) = ${EPS_MAP[rules]} EPS"
      ;;
    sink)
      gen_conf "$TMP/rules_all.wfl" "$SINKS" "100ms" "2147483648" "$TMP/conf_sink.toml"
      measure sink "$TMP/conf_sink.toml" ""
      EPS_MAP[sink]=$(get_eps)
      echo "  → sink(+输出链) = ${EPS_MAP[sink]} EPS"
      ;;
    prof-off)
      gen_conf "$TMP/rules_all.wfl" "$SINKS" "100ms" "2147483648" "$TMP/conf_prof.toml"
      measure profoff "$TMP/conf_prof.toml" "WF_RULE_PROFILING=0"
      EPS_MAP[profoff]=$(get_eps)
      echo "  → prof-off(+计时门控) = ${EPS_MAP[profoff]} EPS"
      ;;
    budget:*)
      PBB="${CUT#budget:}"
      gen_conf "$TMP/rules_all.wfl" "$SINKS" "100ms" "$PBB" "$TMP/conf_budget.toml"
      measure "budget${PBB}" "$TMP/conf_budget.toml" ""
      EPS_MAP["budget:${PBB}"]=$(get_eps)
      echo "  → budget:${PBB}(+预算) = ${EPS_MAP[budget:${PBB}]} EPS"
      ;;
    subset)
      [ -n "$SUBSET" ] || { echo "  subset 刀需 --rule-subset" >&2; continue; }
      "$PY" - "$TMP/rules_all.wfl" "$TMP/rules_subset.wfl" "$SUBSET" <<'PYEOF'
import sys, re
src, out, pat = sys.argv[1], sys.argv[2], sys.argv[3]
rx = re.compile(pat)
sel = set()
for line in open(src):
    if line.startswith('rule '):
        name = line.split()[1]
        if rx.search(name):
            sel.add(name)
seen = False; in_rule = False
lines = []
for line in open(src):
    if line.startswith('rule '):
        seen = True
        name = line.split()[1]
        in_rule = name in sel
        if in_rule: lines.append(line)
        continue
    if not seen or in_rule: lines.append(line)
open(out, 'w').write(''.join(lines))
PYEOF
      # 空 sink 隔离规则成本（与 rules 档同台面可比）
      gen_conf "$TMP/rules_subset.wfl" "$TMP/sinks_empty" "100ms" "2147483648" "$TMP/conf_subset.toml"
      measure subset "$TMP/conf_subset.toml" ""
      EPS_MAP[subset]=$(get_eps)
      echo "  → subset(=${SUBSET}) = ${EPS_MAP[subset]} EPS"
      ;;
    *)
      echo "  ⚠ 未知刀: $CUT" >&2
      ;;
  esac
done

echo ""
echo "==== 墙表（N=$N，load=$(uptime | sed 's/.*averages:/averages:/')）===="
"$PY" - "$TMP/wall.txt" "$N" <<'PYEOF'
import sys, re
wall, n = sys.argv[1], int(sys.argv[2])
best = {}
for line in open(wall):
    m = re.search(r'MEAS (\S+)_best eps=(\d+)', line)
    if m:
        best[m.group(1)] = int(m.group(2))
if not best:
    sys.exit(0)
order = ['floor', 'rules', 'sink', 'profoff']
extra = [k for k in best if k not in order]
order += sorted(extra)
prev = None
print(f"{'cut':<16}{'EPS':>10}{'增量成本(s)':>14}  {'墙判定':<28}")
for k in order:
    if k not in best:
        continue
    eps = best[k]
    cost = ''
    if prev is not None and prev > 0:
        c = n / eps - n / prev
        cost = f'{c:.2f}'
    verdict = ''
    if prev is not None and prev > 0:
        ratio = eps / prev
        if ratio < 0.5:
            verdict = f'← 墙！吞吐 {ratio:.0%}'
        elif ratio < 0.9:
            verdict = f'← 有成本（{ratio:.0%}）'
        else:
            verdict = '（近无成本）'
    print(f"{k:<16}{eps:>10,}{cost:>14}  {verdict:<28}")
    prev = eps
PYEOF
echo ""
echo "==== 全量测量记录 ===="
cat "$TMP/wall.txt"
pkill -9 -f "wfusion daemon" 2>/dev/null; true
