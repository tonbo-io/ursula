"""`dst` CLI: subcommand dispatch.

Run with `python3 -m scripts.dst <subcommand> [args]`.

Subcommand list (verb-first, flat namespace):

  Audits (each returns non-zero on violation; `all` runs every audit):
    nondeterminism      DoD #1 — no silent fallback in sim-reachable code
    pipeline-smoke      DoD #2 — corrupt_*_expectation only in pipeline-smoke families
    layer2              DoD #4 — Layer 2 has failpoints or is doc-retired
    ci-shape            DoD #6 — no multi-element strict-equality jq step matches
    modularity          DoD #3 — ratcheting line budget on madsim_harness/mod.rs
    seed-inventory      DoD #7 + family discipline + per-track seed budget
    failure-guards      every failure-smoke entry has fresh PR CI coverage
    all                 run every audit

  Reports (write target/ artifacts; CI uploads them):
    coverage-report     DoD #8 — distinct stable trace event cardinality
    seed-report         PR/nightly seed inventory snapshot
    result-summary      walk failure artifacts under target/

  Tools:
    throughput          measure seeds/min/core; suggest PR/nightly budgets
"""

from __future__ import annotations

import argparse
import sys

from scripts.dst.audits import (
    AUDITS,
    audit_all,
)
from scripts.dst.reports import (
    report_coverage,
    report_result_summary,
    report_seed_inventory,
)
from scripts.dst.tools import tool_throughput


SUBCOMMANDS = {
    # audits
    **AUDITS,
    "all": audit_all,
    # reports
    "coverage-report": report_coverage,
    "seed-report": report_seed_inventory,
    "result-summary": report_result_summary,
    # tools
    "throughput": tool_throughput,
}


def _print_help() -> None:
    print(__doc__ or "")
    print("Available subcommands:")
    for name in sorted(SUBCOMMANDS):
        print(f"  {name}")


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if not argv or argv[0] in ("-h", "--help"):
        _print_help()
        return 0

    name = argv[0]
    if name not in SUBCOMMANDS:
        print(f"unknown subcommand: {name}\n", file=sys.stderr)
        _print_help()
        return 2

    return SUBCOMMANDS[name](argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
