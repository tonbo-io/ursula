"""DST reporting subcommands: coverage, seed inventory, result summary.

Reports write artifacts under `target/` for CI upload but don't fail
on absence of input (unlike audits, they're observational).
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Iterable

from scripts.dst.common import (
    COVERAGE_BASELINE,
    NIGHTLY_WORKFLOW,
    PR_WORKFLOW,
    ROOT,
    format_items,
    rel,
    write_json,
    write_text,
)


# =============================================================================
# coverage — DoD #8
# =============================================================================

def _event_names(events: Iterable[object]) -> Iterable[str]:
    for ev in events:
        if isinstance(ev, dict):
            name = ev.get("event")
            if isinstance(name, str):
                yield name


def _collect_coverage(scan_dirs: list[Path]) -> dict[str, object]:
    success_seeds: set[int] = set()
    failure_seeds: set[int] = set()
    event_to_seeds: dict[str, set[int]] = {}

    def record(events: list[object], seed: int, kind: str) -> None:
        if kind == "success":
            success_seeds.add(seed)
        else:
            failure_seeds.add(seed)
        for name in _event_names(events):
            event_to_seeds.setdefault(name, set()).add(seed)

    for base in scan_dirs:
        if not base.exists():
            continue
        for path in sorted(base.rglob("seed-*-replay.json")):
            try:
                data = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            seed = ((data.get("schedule") or {}).get("seed")) or data.get("seed")
            events = ((data.get("outcome") or {}).get("trace") or {}).get("events") or []
            if isinstance(seed, int) and isinstance(events, list):
                record(events, seed, "success")
        for path in sorted(base.rglob("seed-*-stable-trace.json")):
            try:
                data = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            seed = (data.get("schedule") or {}).get("seed")
            events = (data.get("stable_trace") or {}).get("events") or []
            if isinstance(seed, int) and isinstance(events, list):
                record(events, seed, "failure")

    event_counts = {name: len(seeds) for name, seeds in event_to_seeds.items()}
    summary: dict[str, object] = {
        "success_seeds": sorted(success_seeds),
        "failure_seeds": sorted(failure_seeds),
        "total_seeds": len(success_seeds | failure_seeds),
        "distinct_event_count": len(event_counts),
        "event_counts": dict(sorted(event_counts.items())),
    }

    if COVERAGE_BASELINE.exists():
        try:
            baseline = json.loads(COVERAGE_BASELINE.read_text())
            baseline_events = set(baseline.get("events", []))
        except (json.JSONDecodeError, OSError):
            baseline_events = set()
        observed = set(event_counts)
        summary["baseline_path"] = rel(COVERAGE_BASELINE)
        summary["new_events"] = sorted(observed - baseline_events)
        summary["regressed_events"] = sorted(baseline_events - observed)
    return summary


def _render_coverage_md(summary: dict[str, object]) -> str:
    lines = ["# DST coverage report", ""]
    lines.append(
        f"- total seeds: **{summary['total_seeds']}** "
        f"(success {len(summary['success_seeds'])}, failure {len(summary['failure_seeds'])})"
    )
    lines.append(f"- distinct stable trace events: **{summary['distinct_event_count']}**")
    if "baseline_path" in summary:
        lines.append(f"- baseline: `{summary['baseline_path']}`")
        new = summary.get("new_events") or []
        lines.append(f"- NEW events: {', '.join(f'`{e}`' for e in new) if new else '(none)'}")
        regressed = summary.get("regressed_events") or []
        lines.append(
            f"- REGRESSED events: {', '.join(f'`{e}`' for e in regressed) if regressed else '(none)'}"
        )
    lines.append("")
    lines.append("## Event coverage (seed count per event)")
    lines.append("")
    lines.append("| event | seeds |")
    lines.append("|-------|------:|")
    for name, count in sorted(
        summary["event_counts"].items(), key=lambda kv: (-kv[1], kv[0])
    ):
        lines.append(f"| `{name}` | {count} |")
    lines.append("")
    return "\n".join(lines) + "\n"


def report_coverage(argv: list[str]) -> int:
    """DoD #8: distinct trace-event cardinality across a seed sweep."""
    parser = argparse.ArgumentParser(prog="dst coverage-report")
    parser.add_argument("--scan", action="append", default=[],
                        help="directory to scan (repeatable); default = target/ursula-sim-failures")
    parser.add_argument("--out-json", default="target/dst-coverage-report.json")
    parser.add_argument("--out-md", default="target/dst-coverage-report.md")
    parser.add_argument("--update-baseline", action="store_true",
                        help=f"overwrite {rel(COVERAGE_BASELINE)} with the observed event set")
    args = parser.parse_args(argv)

    scan_dirs = (
        [ROOT / d for d in args.scan]
        if args.scan
        else [ROOT / "target/ursula-sim-failures"]
    )
    summary = _collect_coverage(scan_dirs)
    write_json(ROOT / args.out_json, summary)
    write_text(ROOT / args.out_md, _render_coverage_md(summary))

    if args.update_baseline:
        events = sorted(summary["event_counts"].keys())
        write_json(COVERAGE_BASELINE, {
            "events": events,
            "_note": (
                "Baseline of stable SimEvent names seen by the nightly sweep. "
                "Re-run `dst coverage-report --update-baseline` after intentional "
                "coverage expansions."
            ),
        })
        print(f"updated baseline with {len(events)} events at {rel(COVERAGE_BASELINE)}")

    print(
        f"DST coverage report:\n"
        f"  total seeds:           {summary['total_seeds']}\n"
        f"  success seeds:         {len(summary['success_seeds'])}\n"
        f"  failure seeds:         {len(summary['failure_seeds'])}\n"
        f"  distinct events seen:  {summary['distinct_event_count']}\n"
        f"  wrote {args.out_json} + {args.out_md}"
    )
    if summary.get("new_events"):
        print(f"  NEW events:            {summary['new_events']}")
    if summary.get("regressed_events"):
        print(f"  REGRESSED events:      {summary['regressed_events']}")
    return 0


# =============================================================================
# seed-report — markdown + json snapshot of PR/nightly seed inventory
# =============================================================================

WORKFLOWS = {"pr": PR_WORKFLOW, "nightly": NIGHTLY_WORKFLOW}


def _parse_smoke_commands(workflow: str) -> list[dict]:
    commands: list[dict] = []
    pattern = re.compile(
        r"cargo run -p ursula-sim --bin ursula-sim -- smoke \\(?P<body>.*?)"
        r"(?=\n\s*RUSTFLAGS=|\n\s*-\s+name:|\Z)",
        re.DOTALL,
    )
    for match in pattern.finditer(workflow):
        body = match.group("body")
        families = re.findall(r"--seed-family\s+([a-z0-9-]+)", body)
        ranges = re.findall(r"--seed-range\s+([0-9]+\.\.=[0-9]+)", body)
        failure_dirs = re.findall(r"--failure-dir\s+([^\s\\]+)", body)
        commands.append({
            "seed_ranges": ranges,
            "seed_families": families,
            "failure_dirs": failure_dirs,
            "expect_failures": "--expect-failures" in body,
        })
    return commands


def _collect_workflow(label: str, path: Path) -> dict:
    workflow = path.read_text()
    commands = _parse_smoke_commands(workflow)
    families = sorted({f for c in commands for f in c["seed_families"]})
    ranges = sorted({r for c in commands for r in c["seed_ranges"]})
    failure_dirs = sorted({d for c in commands for d in c["failure_dirs"]})
    expected_failures = sorted({
        f for c in commands if c["expect_failures"] for f in c["seed_families"]
    })
    return {
        "label": label,
        "workflow": rel(path),
        "smoke_command_count": len(commands),
        "seed_ranges": ranges,
        "seed_families": families,
        "expected_failure_families": expected_failures,
        "failure_dirs": failure_dirs,
        "commands": commands,
    }


def report_seed_inventory(argv: list[str]) -> int:
    """Snapshot of PR/nightly seed inventory as JSON + Markdown."""
    parser = argparse.ArgumentParser(prog="dst seed-report")
    parser.add_argument("--out-json", default="target/ursula-sim-seed-inventory/report.json")
    parser.add_argument("--out-md", default="target/ursula-sim-seed-inventory/report.md")
    args = parser.parse_args(argv)

    report = {
        "schema_version": 1,
        "workflows": [_collect_workflow(label, path) for label, path in WORKFLOWS.items()],
    }
    out_json = ROOT / args.out_json
    out_md = ROOT / args.out_md
    write_json(out_json, report)
    lines = [
        "# Ursula DST Seed Inventory",
        "",
        f"Schema version: `{report['schema_version']}`",
        "",
    ]
    for w in report["workflows"]:
        lines += [
            f"## {w['label']}",
            "",
            f"- Workflow: `{w['workflow']}`",
            f"- Smoke commands: `{w['smoke_command_count']}`",
            f"- Seed ranges: {format_items(w['seed_ranges'])}",
            f"- Seed families: {format_items(w['seed_families'])}",
            f"- Expected-failure families: {format_items(w['expected_failure_families'])}",
            f"- Failure dirs: {format_items(w['failure_dirs'])}",
            "",
        ]
    write_text(out_md, "\n".join(lines))
    print(f"wrote {rel(out_json)}")
    print(f"wrote {rel(out_md)}")
    return 0


# =============================================================================
# result-summary — what failed during a sim sweep
# =============================================================================

def _extract_invariant(panic: str) -> str | None:
    m = re.search(r"invariant `([^`]+)` failed", panic)
    return m.group(1) if m else None


def _load_failure(path: Path) -> dict:
    artifact = json.loads(path.read_text())
    schedule = artifact.get("schedule") or {}
    panic = str(artifact.get("panic", ""))
    return {
        "path": rel(path),
        "dir": rel(path.parent),
        "seed": artifact.get("seed"),
        "scenario": schedule.get("scenario"),
        "stream_id": (schedule.get("workload") or {}).get("stream_id"),
        "invariant": _extract_invariant(panic),
        "panic": panic,
        "stable_trace_path": artifact.get("stable_trace_path"),
        "raw_event_log_path": artifact.get("raw_event_log_path"),
    }


def _collect_failures(inputs: list[Path]) -> list[dict]:
    failures: list[dict] = []
    for base in inputs:
        base = base.resolve()
        if not base.exists():
            continue
        for path in sorted(base.glob("**/seed-*-failure.json")):
            if "-minimized-failure.json" in path.name:
                continue
            try:
                failures.append(_load_failure(path))
            except (json.JSONDecodeError, OSError) as exc:
                failures.append({"path": rel(path), "error": str(exc)})
    return sorted(failures, key=lambda i: (str(i.get("dir", "")), int(i.get("seed") or -1)))


def report_result_summary(argv: list[str]) -> int:
    """Walk failure artifacts under `target/` and summarize them."""
    parser = argparse.ArgumentParser(prog="dst result-summary")
    parser.add_argument("--input", action="append", default=[],
                        help="input directory to scan (repeatable); default = target/")
    parser.add_argument("--out-json", default="target/ursula-sim-result-summary/report.json")
    parser.add_argument("--out-md", default="target/ursula-sim-result-summary/report.md")
    args = parser.parse_args(argv)

    inputs = [Path(p).resolve() for p in args.input] if args.input else [ROOT / "target"]
    failures = _collect_failures(inputs)
    summary = {
        "schema_version": 1,
        "inputs": [str(p) for p in inputs],
        "failure_count": len(failures),
        "failures": failures,
    }
    out_json = ROOT / args.out_json
    out_md = ROOT / args.out_md
    write_json(out_json, summary)

    lines = [
        "# Ursula DST Result Summary",
        "",
        f"Schema version: `{summary['schema_version']}`",
        f"Failure artifacts: `{len(failures)}`",
        "",
    ]
    if not failures:
        lines.append("No failure artifacts were found.")
    else:
        by_dir: dict[str, list[dict]] = {}
        for f in failures:
            by_dir.setdefault(str(f.get("dir", "unknown")), []).append(f)
        for d, items in sorted(by_dir.items()):
            lines += [f"## `{d}`", ""]
            for item in items:
                seed = item.get("seed")
                invariant = item.get("invariant") or "unknown invariant"
                scenario = item.get("scenario") or "unknown scenario"
                lines.append(f"- seed `{seed}` `{scenario}` `{invariant}`")
            lines.append("")
    write_text(out_md, "\n".join(lines))
    print(f"wrote {rel(out_json)}")
    print(f"wrote {rel(out_md)}")
    print(f"failure artifacts: {len(failures)}")
    return 0
