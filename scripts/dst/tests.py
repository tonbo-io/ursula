"""Smoke tests for the dst CLI package.

Run with `python3 -m pytest scripts/dst/tests.py` from the repo root.

These don't try to be a full unit-test suite — they verify:
  - every subcommand is dispatchable
  - the 9 audits parse/scan without crashing and return a valid exit code
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


@pytest.mark.parametrize(
    "packages",
    [
        # The patch was dropped: cargo resolved the published crate.
        [
            {"name": "futures-macro", "version": "0.3.32", "source": "registry+https://github.com/rust-lang/crates.io-index"},
            {"name": "futures-util", "version": "0.3.32", "source": "registry+https://github.com/rust-lang/crates.io-index"},
        ],
        # futures-util moved past the vendored version.
        [
            {"name": "futures-macro", "version": "0.3.32"},
            {"name": "futures-util", "version": "0.3.33", "source": "registry+https://github.com/rust-lang/crates.io-index"},
        ],
    ],
)
def test_sim_patches_audit_rejects_a_dropped_patch(monkeypatch, packages):
    monkeypatch.setattr(audits, "_lock_packages", lambda: packages)
    assert audits.audit_sim_patches([]) == 1


def _crate_file(root, rel_path: str, body: str) -> None:
    path = root / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


def test_nondeterminism_flags_an_unbiased_tokio_select(monkeypatch, tmp_path):
    _crate_file(tmp_path, "crates/demo/src/lib.rs", (
        "async fn a() { tokio::select! { _ = x() => {} } }\n"
        "async fn b() { crate::rt::select! {\n    biased;\n    _ = x() => {}\n} }\n"
        "async fn c() { futures::select! { _ = x() => {} } }\n"
    ))
    monkeypatch.setattr(audits, "ROOT", tmp_path)
    findings = audits._scan_nondeterminism()
    assert [(f.category, f.line) for f in findings] == [("unbiased_select", 1)]


def test_nondeterminism_flags_a_madsim_runtime_outside_run_isolated(monkeypatch, tmp_path):
    _crate_file(tmp_path, "crates/ursula-raft/src/sim_runtime.rs", (
        "pub fn run_isolated() { madsim::runtime::Runtime::with_seed_and_config(1, c); }\n"
    ))
    _crate_file(tmp_path, "crates/demo/src/tests.rs", (
        "fn helper() { madsim::runtime::Runtime::with_seed_and_config(1, c).block_on(f); }\n"
    ))
    monkeypatch.setattr(audits, "ROOT", tmp_path)
    misplaced = audits._misplaced_madsim_runtimes()
    assert len(misplaced) == 1 and "crates/demo/src/tests.rs:1" in misplaced[0]


@pytest.mark.parametrize(
    "old, new",
    [
        ('--shard "$DST_SHARD"', "--shard 0/7"),
        ("      DST_SHARD: ${{ strategy.job-index }}/${{ strategy.job-total }}\n", ""),
        ("needs: [audits, sweep, ", "needs: [audits, "),
        ("\n    if: always()\n", "\n"),
        ('all(.[]; .result == "success")', 'any(.[]; .result == "success")'),
    ],
)
def test_shard_wiring_flags_a_run_that_could_pass_in_part(old, new):
    text = common.NIGHTLY_WORKFLOW.read_text()
    assert audits._shard_wiring_errors(text) == []
    assert old in text
    assert audits._shard_wiring_errors(text.replace(old, new, 1))


def test_sim_test_filters_count_only_nextest_commands(tmp_path):
    workflow = tmp_path / "dst-nightly.yml"
    workflow.write_text(
        'RUSTFLAGS="--cfg madsim" cargo nextest run -p ursula-sim --lib --no-fail-fast -- \\\n'
        "  recovery_tests poison_marker\n"
        'RUSTFLAGS="--cfg madsim" cargo test -p ursula-sim --lib -- _replays\n'
    )
    assert audits._sim_test_filters(workflow) == {"recovery_tests", "poison_marker"}
