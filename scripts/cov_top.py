#!/usr/bin/env python3
"""Parse llvm-cov summary report and print files with most missed lines."""
import sys

rows = []
for line in open(sys.argv[1] if len(sys.argv) > 1 else '/tmp/cov_report.txt'):
    if '/src/' not in line:
        continue
    parts = line.split()
    if len(parts) < 6:
        continue
    try:
        # llvm-cov summary columns: filename, Regions, MissedRegions, RegionCover%,
        # Functions, MissedFunctions, Executed%, Lines, MissedLines, LineCover%, ...
        lines = int(parts[7])
        miss = int(parts[8])
        cov = lines - miss
        pct = float(parts[9].rstrip('%'))
    except (ValueError, IndexError):
        continue
    rows.append((miss, pct, parts[0], lines, cov))

rows.sort(reverse=True)
for miss, pct, f, lines, cov in rows[:30]:
    print(f"{miss:6d}  {pct:6.2f}%  {f}  (lines={lines} cov={cov})")
print("---")
print(f"top30 total missing: {sum(r[0] for r in rows[:30])}")
print(f"all files total missing: {sum(r[0] for r in rows)}")
