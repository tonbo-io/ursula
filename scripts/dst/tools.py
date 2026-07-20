"""DST measurement / one-shot tooling subcommands."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

from scripts.dst.common import ROOT


def _run_one_seed(family: str, seed: int, failure_dir: Path) -> None:
    cmd = [
        "cargo", "run", "--quiet", "-p", "ursula-sim",
        "--bin", "ursula-sim", "--", "smoke",
        "--seed", str(seed),
        "--failure-dir", str(failure_dir),
    ]
    if family:
        cmd[-4:-4] = ["--seed-family", family]
    env = os.environ.copy()
    env.setdefault("RUSTFLAGS", "--cfg madsim")
    subprocess.run(cmd, check=False, env=env, cwd=ROOT, capture_output=True)


def tool_throughput(argv: list[str]) -> int:
    """Measure seeds/min/core so the PR/nightly budget is data-driven.

    DoD #7. Wall-clocks `ursula-sim smoke --seed N` for `--measure` seeds
    (after `--warmup` warmup seeds), divides total elapsed by seed count, and
    suggests PR (2 min) and nightly (30 min) seed budgets.
    """
    parser = argparse.ArgumentParser(prog="dst throughput")
    parser.add_argument("--family", default="",
                        help="seed family to run (default: empty, uses --seed alone)")
    parser.add_argument("--warmup", type=int, default=3,
                        help="warmup runs (not measured); default 3")
    parser.add_argument("--measure", type=int, default=10,
                        help="measured runs; default 10")
    parser.add_argument("--start-seed", type=int, default=10_000,
                        help="first seed to run (warmup + measure are sequential from here)")
    parser.add_argument("--json", action="store_true", help="emit a JSON summary")
    args = parser.parse_args(argv)

    failure_dir = ROOT / "target/ursula-sim-throughput"
    failure_dir.mkdir(parents=True, exist_ok=True)

    seeds = list(range(args.start_seed, args.start_seed + args.warmup + args.measure))
    for seed in seeds[: args.warmup]:
        _run_one_seed(args.family, seed, failure_dir)

    started = time.perf_counter()
    for seed in seeds[args.warmup :]:
        _run_one_seed(args.family, seed, failure_dir)
    elapsed_s = time.perf_counter() - started

    seeds_per_minute = (args.measure / elapsed_s * 60.0) if elapsed_s > 0 else 0.0
    pr_budget_min = 2.0
    nightly_budget_min = 30.0

    summary = {
        "family": args.family or "<none>",
        "warmup": args.warmup,
        "measure": args.measure,
        "elapsed_s": round(elapsed_s, 3),
        "seeds_per_minute_per_core": round(seeds_per_minute, 2),
        "pr_budget_minutes": pr_budget_min,
        "nightly_budget_minutes": nightly_budget_min,
        "pr_seed_count_suggested": int(seeds_per_minute * pr_budget_min),
        "nightly_seed_count_suggested": int(seeds_per_minute * nightly_budget_min),
        "note": (
            "Lock these numbers into the `Seed Throughput Budget` section of "
            "docs/architecture/deterministic-simulation-testing.md and have "
            "`dst seed-inventory` enforce the per-track seed budget."
        ),
    }

    if args.json:
        print(json.dumps(summary, indent=2))
        return 0

    print(f"family:           {summary['family']}")
    print(f"warmup x measure: {args.warmup} x {args.measure}")
    print(f"elapsed:          {elapsed_s:.2f}s")
    print(f"seeds/min/core:   {seeds_per_minute:.2f}")
    print()
    print(f"PR budget {pr_budget_min:>4.1f} min  -> {summary['pr_seed_count_suggested']:>4} seeds")
    print(f"Nightly   {nightly_budget_min:>4.1f} min  -> {summary['nightly_seed_count_suggested']:>4} seeds")
    print()
    print(summary["note"])
    return 0
