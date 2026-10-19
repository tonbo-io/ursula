"""`dst` CLI: subcommand dispatch.

Run with `python3 -m scripts.dst <subcommand> [args]`.

Subcommand list (verb-first, flat namespace):

  Audits (each returns non-zero on violation; `all` runs every audit):
    nondeterminism      no silent fallback in sim-reachable code
    pipeline-smoke      corrupt_*_expectation only in pipeline-smoke families
    layer2              no fail_point! macros in crates/
    ci-shape            no multi-element strict-equality jq step matches
    modularity          ratcheting line budget on madsim_harness/mod.rs
    seed-inventory      family discipline and seed budget
    failure-guards      every failure-smoke entry has fresh nightly coverage
    sim-patches         vendored madsim patches match the crate graph
    all                 run every audit

  Reports (write target/ artifacts; CI uploads them):
    coverage-report     distinct stable trace event cardinality
    seed-report         nightly seed inventory snapshot
    result-summary      walk failure artifacts under target/

  Tools:
    throughput          measure seeds/min/core; suggest the nightly budget
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
