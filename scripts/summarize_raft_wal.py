#!/usr/bin/env python3
"""Summarize independent Criterion WAL runs into JSON and Markdown."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: summarize_raft_wal.py ARTIFACT_ROOT")
    root = Path(sys.argv[1])
    samples: dict[str, list[float]] = defaultdict(list)
    for estimate_path in sorted(root.glob("run-*/criterion/**/new/estimates.json")):
        benchmark = estimate_path.parent.parent.relative_to(
            estimate_path.parents[3]
        ).as_posix()
        estimates = json.loads(estimate_path.read_text())
        samples[benchmark].append(estimates["mean"]["point_estimate"] / 1_000_000)

    if not samples:
        raise SystemExit(f"no Criterion estimates found under {root}")

    summary = {}
    for benchmark, values in sorted(samples.items()):
        mean_ms = statistics.fmean(values)
        stdev_ms = statistics.stdev(values) if len(values) > 1 else 0.0
        summary[benchmark] = {
            "runs": len(values),
            "mean_ms": mean_ms,
            "stdev_ms": stdev_ms,
            "coefficient_of_variation_percent": (
                stdev_ms / mean_ms * 100 if mean_ms else 0.0
            ),
            "run_mean_ms": values,
        }

    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        "# Raft WAL microbenchmark summary",
        "",
        "| Benchmark | Runs | Mean (ms) | Stddev (ms) | CV |",
        "|---|---:|---:|---:|---:|",
    ]
    for benchmark, result in summary.items():
        lines.append(
            f"| `{benchmark}` | {result['runs']} | {result['mean_ms']:.3f} | "
            f"{result['stdev_ms']:.3f} | "
            f"{result['coefficient_of_variation_percent']:.2f}% |"
        )
    lines.append("")
    (root / "summary.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
