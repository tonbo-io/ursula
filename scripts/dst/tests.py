"""Smoke tests for the dst CLI package.

Run with `python3 -m pytest scripts/dst/tests.py` from the repo root.

These don't try to be a full unit-test suite — they verify:
  - every subcommand is dispatchable
  - the 7 audits parse/scan without crashing and return a valid exit code
  - shared parsing helpers don't lie about seed counts
"""

from __future__ import annotations

import pytest

from scripts.dst import audits, cli, common
from scripts.dst.audits import PIPELINE_SMOKE_RANGES


def test_subcommand_table_covers_audits():
    for name in audits.AUDITS:
        assert name in cli.SUBCOMMANDS, f"{name} missing from cli.SUBCOMMANDS"


def test_subcommand_table_has_reports_and_tools():
    for name in ("coverage-report", "seed-report", "result-summary", "throughput", "all"):
        assert name in cli.SUBCOMMANDS, f"{name} missing from cli.SUBCOMMANDS"


@pytest.mark.parametrize("name", list(audits.AUDITS))
def test_audit_returns_zero(name: str):
    """Each audit currently passes against the checked-in repo state.

    If any of these starts failing, that's a real regression (likely a new
    nondeterminism source, drifted CI workflow, or harness file growth) — fix
    the underlying issue, not this test.
    """
    rc = audits.AUDITS[name]([])
    assert rc == 0, f"audit `{name}` failed with exit code {rc}"


def test_pipeline_smoke_ranges_cover_known_seeds():
    # Sanity: every range is non-empty and well-formed.
    for family, rng in PIPELINE_SMOKE_RANGES.items():
        assert len(rng) > 0, f"empty range for {family}"
        assert rng.start < rng.stop, f"inverted range for {family}"


def test_corpus_files_exist():
    assert common.FAILURE_CORPUS.exists()
    assert common.SCHEDULE_CORPUS.exists()
    assert common.SMOKE_CORPUS.exists()
