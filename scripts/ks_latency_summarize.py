#!/usr/bin/env python3
"""Summarise ks_latency_matrix.sh output into Markdown tables.

  ks_latency_summarize.py <results_dir> [label ...]

For each (label, background, N) cell it pools the raw samples of every run
(`*.samples`) and prints mean/p50/p99/p999 over the pooled set, the per-run
spread of the mean, commits/s (mean over runs) and the 412/error totals.
"""

import glob
import json
import os
import re
import statistics
import sys

NAME = re.compile(r"^(?P<label>.+)-bg(?P<bg>[01])-n(?P<n>\d+)-r(?P<run>\d+)\.json$")


def pct(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))]


def main(argv):
    root = argv[1]
    labels = set(argv[2:])
    cells = {}
    for path in sorted(glob.glob(os.path.join(root, "*.json"))):
        m = NAME.match(os.path.basename(path))
        if not m or (labels and m["label"] not in labels):
            continue
        key = (m["label"], int(m["bg"]), int(m["n"]))
        with open(path) as fh:
            d = json.load(fh)
        samples_path = path[: -len(".json")] + ".samples"
        samples = []
        if os.path.exists(samples_path):
            with open(samples_path) as fh:
                samples = [int(line.split()[0]) / 1000.0 for line in fh if line.strip()]
        bg_rate = None
        bg_path = path[: -len(".json")] + ".bg.json"
        if os.path.exists(bg_path):
            try:
                with open(bg_path) as fh:
                    bg = json.load(fh)
                bg_rate = bg.get("aggregate_ops_per_sec")
            except (OSError, ValueError):
                bg_rate = None
        cells.setdefault(key, []).append((d, samples, bg_rate))

    print("| config | bg | N | runs | mean ms (run spread) | p50 | p99 | p999 | max | commits/s | bg appends/s | 412 | errors | load avg (1m) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(cells):
        label, bg, n = key
        runs = cells[key]
        pooled = sorted(s for _, samples, _ in runs for s in samples)
        means = [d["latency_ms"]["mean_ms"] for d, _, _ in runs]
        cps = statistics.mean(d["commits_per_sec"] for d, _, _ in runs)
        p412 = sum(d["precondition_failed"] for d, _, _ in runs)
        errs = sum(sum(d["errors"].values()) for d, _, _ in runs)
        loads = [d.get("load_avg_at_start", "?").split()[0] for d, _, _ in runs]
        bg_rates = [r for _, _, r in runs if r]
        bg_txt = f"{statistics.mean(bg_rates):.0f}" if bg_rates else "-"
        if pooled:
            mean = statistics.mean(pooled)
            p50, p99, p999, mx = pct(pooled, 0.5), pct(pooled, 0.99), pct(pooled, 0.999), pooled[-1]
        else:
            mean = statistics.mean(means)
            p50 = p99 = p999 = mx = float("nan")
        print(
            f"| {label} | {'yes' if bg else 'no'} | {n} | {len(runs)} | {mean:.2f} ({min(means):.2f}-{max(means):.2f}) "
            f"| {p50:.2f} | {p99:.2f} | {p999:.2f} | {mx:.1f} | {cps:.0f} | {bg_txt} | {p412} | {errs} | {'/'.join(loads)} |"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
