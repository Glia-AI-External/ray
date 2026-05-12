#!/usr/bin/env python3
"""Summarize results/optimization_perf_four_tree.jsonl into a 4-tree table.

Configs: master, v5 (pre-fix), no_m5, v5_fix.
Reports mean wall ± stdev, deltas vs master and vs v5, and hash agreement.
"""
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

CONFIGS = ["master", "v5", "no_m5", "v5_fix"]


def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def summarize(rows):
    by = defaultdict(list)
    for r in rows:
        by[(r["config"], r["workload"])].append(r)
    out = {}
    for k, rs in by.items():
        walls = [r["wall_time_sec"] for r in rs]
        hashes = {r.get("output_hash") for r in rs}
        out[k] = {
            "n": len(rs),
            "mean": statistics.mean(walls),
            "stdev": statistics.stdev(walls) if len(walls) > 1 else 0.0,
            "min": min(walls),
            "max": max(walls),
            "hashes": hashes,
        }
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "jsonl",
        nargs="?",
        default="results/optimization_perf_four_tree.jsonl",
    )
    args = parser.parse_args()
    rows = load(args.jsonl)
    s = summarize(rows)

    workloads = sorted({w for _, w in s.keys()}, key=lambda x: (x == "long_tasks", x))

    # Per-workload wall table
    print(f"{'workload':22s}  {'master':>14s}  {'v5':>14s}  {'no_m5':>14s}  {'v5_fix':>14s}   {'Δ v5_fix vs master':>20s}  {'Δ v5_fix vs v5':>17s}  {'hashes':>8s}")
    print("-" * 145)
    for wl in workloads:
        cells = {}
        all_hashes = set()
        for cfg in CONFIGS:
            row = s.get((cfg, wl))
            if row is None:
                cells[cfg] = (None, None)
                continue
            cells[cfg] = (row["mean"], row["stdev"])
            all_hashes |= row["hashes"]

        def fmt(c):
            m, sd = cells[c]
            if m is None:
                return "    --       "
            return f"{m:6.2f}±{sd:5.2f}s"

        m_mean = cells["master"][0]
        v_mean = cells["v5"][0]
        f_mean = cells["v5_fix"][0]
        d_fix_master = (f_mean - m_mean) / m_mean * 100 if m_mean else 0
        d_fix_v5 = (f_mean - v_mean) / v_mean * 100 if v_mean else 0

        hash_status = "match" if len(all_hashes) == 1 else f"DIFF({len(all_hashes)})"

        print(
            f"{wl:22s}  {fmt('master'):>14s}  {fmt('v5'):>14s}  {fmt('no_m5'):>14s}  {fmt('v5_fix'):>14s}   "
            f"{d_fix_master:+18.1f}%   {d_fix_v5:+15.1f}%   {hash_status:>8s}"
        )

    print()
    # Also: a "neighborhood-of-noise" summary — how much v5_fix differs from v5
    print("Note: v5_fix vs v5 on devpod measures the fix's per-dispatch overhead")
    print("on single-node workloads (no cluster autoscaling exercised). Expected: ≈ 0%.")


if __name__ == "__main__":
    main()
