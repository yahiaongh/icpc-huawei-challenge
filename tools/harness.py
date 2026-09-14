#!/usr/bin/env python3
"""Run judge across (binary, spec) combos and print a table.

Usage: harness.py <spec1> <spec2> ... -- <bin1> <bin2> ...
"""
import sys, subprocess, os

def main():
    args = sys.argv[1:]
    sep = args.index('--')
    specs = args[:sep]
    bins = args[sep+1:]
    print("%-28s | " % "spec" + " | ".join("%-14s" % os.path.basename(b) for b in bins))
    print("-" * (30 + 16 * len(bins)))
    for sp in specs:
        row = ["%-28s" % os.path.basename(sp)]
        for b in bins:
            out = subprocess.run(["python3", "tools/judge.py", sp, b],
                                 capture_output=True, text=True, timeout=600).stdout.strip()
            score = "ERR"
            for line in out.splitlines():
                if "points=" in line:
                    score = line.split("points=")[-1].strip()
            row.append("%-14s" % score)
        print(" | ".join(row))

if __name__ == '__main__':
    main()