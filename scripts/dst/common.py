"""Shared paths, IO helpers, and small utilities used by every DST subcommand."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]


# ---- canonical paths --------------------------------------------------------

CORPUS_DIR = ROOT / "crates/ursula-sim/corpus"
SMOKE_CORPUS = CORPUS_DIR / "smoke.json"
SCHEDULE_CORPUS = CORPUS_DIR / "schedule-smoke.json"
FAILURE_CORPUS = CORPUS_DIR / "failure-smoke.json"
COVERAGE_BASELINE = CORPUS_DIR / "coverage-baseline.json"

PR_WORKFLOW = ROOT / ".github/workflows/ci.yml"
NIGHTLY_WORKFLOW = ROOT / ".github/workflows/dst-nightly.yml"
SMOKE_RS = ROOT / "crates/ursula-sim/src/bin/ursula-sim/smoke.rs"
HARNESS_ROOT = ROOT / "crates/ursula-sim/src/madsim_harness"
DST_DOC = ROOT / "docs/architecture/deterministic-simulation-testing.md"

NONDETERMINISM_WHITELIST = Path(__file__).resolve().parent / "nondeterminism_whitelist.json"


# ---- JSON helpers -----------------------------------------------------------

def load_json(path: Path) -> object:
    return json.loads(path.read_text())


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


# ---- format helpers ---------------------------------------------------------

def format_items(items: Iterable[str]) -> str:
    items = list(items)
    if not items:
        return "_none_"
    return ", ".join(f"`{item}`" for item in items)


def rel(path: Path) -> str:
    """Format `path` relative to repo root for readable output."""
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)
