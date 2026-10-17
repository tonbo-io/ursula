"""DST audit subcommands.

Each `audit_*` function returns a Unix exit code (0 = pass, non-zero = fail).
Audits are invoked by `scripts.dst.cli` and from CI workflows.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from scripts.dst.common import (
    FAILURE_CORPUS,
    HARNESS_ROOT,
    NIGHTLY_WORKFLOW,
    NONDETERMINISM_WHITELIST,
    PR_WORKFLOW,
    ROOT,
    SCHEDULE_CORPUS,
    SMOKE_RS,
    load_json,
    rel,
)


# =============================================================================
# 1. nondeterminism
# =============================================================================

EXEMPT_PATH_PREFIXES = (
    "crates/ursula-bench/",
    "crates/ursula-sim/",
    # Measurement harness (bounded-stream-state.md, State-probe harness): times its own
    # workloads and spawns its own runtime; never built under cfg(madsim).
    "crates/ursula-state-probe/",
)
EXEMPT_PATH_SUBSTRINGS = (
    "/tests/",
    "/benches/",
    "/src/bin/",
)


@dataclass(frozen=True)
class NondetCategory:
    name: str
    description: str
    pattern: re.Pattern[str]


NONDET_CATEGORIES: tuple[NondetCategory, ...] = (
    NondetCategory(
        name="system_time_now",
        description="std::time::SystemTime::now — wall clock, not virtualizable by madsim",
        pattern=re.compile(r"SystemTime\s*::\s*now"),
    ),
    NondetCategory(
        name="std_time_instant_import",
        description=(
            "`use std::time::Instant` — bare Instant::now() in this file is real "
            "wall-clock under cfg(madsim). Switch to `crate::rt::time::Instant`."
        ),
        pattern=re.compile(r"use\s+std::time::\{[^}]*\bInstant\b|use\s+std::time::Instant\b"),
    ),
    NondetCategory(
        name="std_time_instant_qualified",
        description="Inline `std::time::Instant::now()` — same problem, but fully qualified.",
        pattern=re.compile(r"std::time::Instant\s*::\s*now"),
    ),
    NondetCategory(
        name="tokio_runtime_builder",
        description="tokio::runtime::Builder / Runtime::new — opaque OS scheduler under cfg(madsim)",
        pattern=re.compile(
            r"tokio::runtime::Builder|tokio::runtime::Runtime::new\b|Runtime\s*::\s*builder\(\)"
        ),
    ),
    NondetCategory(
        name="std_thread_spawn",
        description="std::thread::Builder / std::thread::spawn — escapes the madsim scheduler",
        pattern=re.compile(
            r"std::thread::Builder|std::thread::spawn|\bthread::spawn\b|\bthread::Builder\b"
        ),
    ),
    NondetCategory(
        name="std_thread_sleep",
        description="std::thread::sleep — blocks the madsim scheduler thread",
        pattern=re.compile(r"\bthread::sleep\b|std::thread::sleep"),
    ),
    NondetCategory(
        name="tokio_watch",
        description=(
            "tokio::sync::watch — parks receivers on Notify slots tokio's thread RNG picks, "
            "and a process-wide counter seeds that RNG, so the wake order of several "
            "receivers depends on the process (#638). Use `crate::rt::sync::watch` "
            "(ursula-raft's sim_watch under cfg(madsim))."
        ),
        pattern=re.compile(r"\b(?:sim_)?tokio::sync::watch\b"),
    ),
)

# A tokio `select!` without `biased;` starts at a branch tokio's thread RNG
# draws, and a process-wide counter seeds that RNG (#638). futures' `select!`
# is patched (third_party/futures-macro) and not matched here.
UNBIASED_SELECT = NondetCategory(
    name="unbiased_select",
    description=(
        "tokio `select!` without `biased;` — its first branch comes from tokio's "
        "process-seeded thread RNG (#638). Add `biased;`."
    ),
    pattern=re.compile(r"\b(?:sim_tokio|tokio|rt)::select!\s*\{(?!\s*biased\s*;)"),
)


@dataclass(frozen=True)
class NondetFinding:
    category: str
    path: str
    line: int
    snippet: str

    def as_dict(self) -> dict[str, object]:
        return {
            "category": self.category,
            "path": self.path,
            "line": self.line,
            "snippet": self.snippet,
        }


def _is_exempt(rel_path: str) -> bool:
    if any(rel_path.startswith(p) for p in EXEMPT_PATH_PREFIXES):
        return True
    if any(s in rel_path for s in EXEMPT_PATH_SUBSTRINGS):
        return True
    if rel_path.endswith("/tests.rs") or rel_path.endswith("tests.rs"):
        return True
    return False


def _iter_source_files() -> Iterable[Path]:
    for path in sorted((ROOT / "crates").rglob("*.rs")):
        rp = path.relative_to(ROOT).as_posix()
        if _is_exempt(rp):
            continue
        yield path


def _scan_nondeterminism() -> list[NondetFinding]:
    findings: list[NondetFinding] = []
    for path in _iter_source_files():
        rp = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, raw in enumerate(text.splitlines(), start=1):
            stripped = raw.strip()
            if stripped.startswith("//") or stripped.startswith("///") or stripped.startswith("//!"):
                continue
            for category in NONDET_CATEGORIES:
                if category.pattern.search(raw):
                    findings.append(
                        NondetFinding(category.name, rp, lineno, stripped)
                    )
                    break
        for m in UNBIASED_SELECT.pattern.finditer(text):
            lineno = text.count("\n", 0, m.start()) + 1
            line = text.splitlines()[lineno - 1].strip()
            if line.startswith("//"):
                continue
            findings.append(NondetFinding(UNBIASED_SELECT.name, rp, lineno, line))
    return findings


# A madsim runtime starts only on a fresh thread: `run_isolated` in
# ursula-raft's sim_runtime.rs, which `ursula-sim`'s `run_with_madsim` and the
# crate's madsim tests call (#638). madsim's own `Runtime::check_determinism`
# and `#[madsim::test]` spawn a thread per run and are not matched.
MADSIM_RUNTIME_RE = re.compile(
    r"\bRuntime\s*::\s*with_seed_and_config\b|madsim::runtime::Runtime\s*::\s*new\b"
)
MADSIM_RUNTIME_HOMES: dict[str, set[str]] = {
    "crates/ursula-raft/src/sim_runtime.rs": {
        "run_isolated",
        # OpenRaft's `AsyncRuntime::block_on`, which only OpenRaft's runtime
        # suite calls (`madsim_openraft_runtime_passes_the_openraft_runtime_suite`).
        "block_on",
    },
}
FN_RE = re.compile(r"\bfn\s+(\w+)")


def _misplaced_madsim_runtimes() -> list[str]:
    """Every madsim runtime built outside its homes, as `path:line  snippet`."""
    misplaced = []
    for path in sorted((ROOT / "crates").rglob("*.rs")):
        rp = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        for m in MADSIM_RUNTIME_RE.finditer(text):
            lineno = text.count("\n", 0, m.start()) + 1
            line = lines[lineno - 1].strip()
            if line.startswith("//"):
                continue
            enclosing = FN_RE.findall(text[: m.start()])
            if enclosing and enclosing[-1] in MADSIM_RUNTIME_HOMES.get(rp, set()):
                continue
            misplaced.append(f"  {rp}:{lineno}  {line}")
    return misplaced


@dataclass(frozen=True)
class WhitelistEntry:
    category: str
    path: str
    snippet_contains: str
    reason: str
    plan: str

    @classmethod
    def from_dict(cls, raw: dict) -> "WhitelistEntry":
        try:
            return cls(
                category=raw["category"],
                path=raw["path"],
                snippet_contains=raw["snippet_contains"],
                reason=raw["reason"],
                plan=raw["plan"],
            )
        except KeyError as exc:
            raise SystemExit(f"whitelist entry missing field {exc}: {raw!r}") from exc

    def matches(self, finding: NondetFinding) -> bool:
        return (
            self.category == finding.category
            and self.path == finding.path
            and self.snippet_contains in finding.snippet
        )


def _load_whitelist() -> list[WhitelistEntry]:
    if not NONDETERMINISM_WHITELIST.exists():
        return []
    raw = load_json(NONDETERMINISM_WHITELIST)
    if not isinstance(raw, list):
        raise SystemExit(f"whitelist root must be a list: {NONDETERMINISM_WHITELIST}")
    return [WhitelistEntry.from_dict(item) for item in raw]


def _render_nondet_markdown(findings: list[NondetFinding]) -> str:
    lines = ["# DST nondeterminism audit", ""]
    if not findings:
        lines.append("No sim-reachable nondeterminism findings.")
        return "\n".join(lines) + "\n"
    by_cat: dict[str, list[NondetFinding]] = {}
    for f in findings:
        by_cat.setdefault(f.category, []).append(f)
    for cat in (*NONDET_CATEGORIES, UNBIASED_SELECT):
        hits = by_cat.get(cat.name, [])
        lines.append(f"## {cat.name} ({len(hits)})")
        lines.append("")
        lines.append(f"_{cat.description}_")
        lines.append("")
        if not hits:
            lines.append("- (none)")
            lines.append("")
            continue
        lines.append("| path | line | snippet |")
        lines.append("|------|-----:|---------|")
        for hit in hits:
            snippet = hit.snippet.replace("|", "\\|")
            if len(snippet) > 80:
                snippet = snippet[:77] + "..."
            lines.append(f"| `{hit.path}` | {hit.line} | `{snippet}` |")
        lines.append("")
    return "\n".join(lines) + "\n"


def audit_nondeterminism(argv: list[str]) -> int:
    """Sim-reachable code has no silent nondeterminism."""
    parser = argparse.ArgumentParser(prog="dst nondeterminism")
    parser.add_argument("--update-baseline", action="store_true",
                        help="overwrite the whitelist with current findings")
    parser.add_argument("--report", action="store_true",
                        help="print a Markdown summary instead of running the check")
    parser.add_argument("--json", action="store_true",
                        help="print raw findings as JSON")
    args = parser.parse_args(argv)

    findings = _scan_nondeterminism()

    if args.update_baseline:
        entries = [
            {
                "category": f.category,
                "path": f.path,
                "snippet_contains": f.snippet[:120],
                "reason": "BASELINE — classify before next DST PR",
                "plan": "gate cfg(not(madsim)), move to madsim seam, or justify exemption",
            }
            for f in findings
        ]
        NONDETERMINISM_WHITELIST.write_text(json.dumps(entries, indent=2) + "\n")
        print(f"wrote {len(findings)} entries to {rel(NONDETERMINISM_WHITELIST)}")
        return 0

    if args.report:
        sys.stdout.write(_render_nondet_markdown(findings))
        return 0

    if args.json:
        sys.stdout.write(json.dumps([f.as_dict() for f in findings], indent=2) + "\n")
        return 0

    whitelist = _load_whitelist()
    unaccounted = [f for f in findings if not any(w.matches(f) for w in whitelist)]
    stale = [w for w in whitelist if not any(w.matches(f) for f in findings)]

    if unaccounted:
        print("ERROR: new nondeterminism findings without whitelist entries:")
        for f in unaccounted:
            print(f"  [{f.category}] {f.path}:{f.line}  {f.snippet}")
        print()
        print(
            "Fix the source (cfg-gate, move to madsim seam, or pick a deterministic\n"
            f"alternative) or add a whitelist entry to {rel(NONDETERMINISM_WHITELIST)}."
        )
        return 1
    if stale:
        print("ERROR: stale whitelist entries (source no longer matches):")
        for w in stale:
            print(f"  [{w.category}] {w.path}  contains={w.snippet_contains!r}")
        return 1

    misplaced = _misplaced_madsim_runtimes()
    if misplaced:
        print("ERROR: madsim runtimes built outside `run_isolated` (ursula-raft sim_runtime.rs):")
        for line in misplaced:
            print(line)
        print()
        print(
            "A runtime on a thread an earlier run used inherits its thread-locals (#638).\n"
            "Run the workload through `ursula_raft::run_isolated` (ursula-sim: `run_with_madsim`)."
        )
        return 1

    print(
        f"OK: {len(findings)} nondeterminism finding(s), all covered by "
        f"{len(whitelist)} whitelist entr{'y' if len(whitelist) == 1 else 'ies'}."
    )
    print("OK: every madsim runtime starts in `run_isolated`.")
    return 0


# =============================================================================
# 2. pipeline-smoke
# =============================================================================

PIPELINE_SMOKE_RANGES: dict[str, range] = {
    "pipeline-smoke-runtime-interleaving-read-corruption": range(172, 177),
    "pipeline-smoke-runtime-raft-network-randomized-read-corruption": range(242, 247),
    "pipeline-smoke-runtime-raft-network-partial-read-corruption": range(247, 252),
    "pipeline-smoke-runtime-raft-network-leader-failover-read-corruption": range(252, 257),
    "pipeline-smoke-http-producer-retry-corruption": range(262, 267),
    "pipeline-smoke-http-live-sse-corruption": range(267, 272),
    "pipeline-smoke-http-live-waiter-corruption": range(272, 277),
    "pipeline-smoke-http-protocol-surface-randomized-corruption": range(297, 302),
    "pipeline-smoke-http-protocol-surface-randomized-sse-corruption": range(302, 307),
    "pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption": range(307, 312),
    "pipeline-smoke-http-snapshot-body-corruption": range(332, 337),
    "pipeline-smoke-runtime-raft-network-tail-read-corruption": range(337, 342),
    "pipeline-smoke-runtime-raft-network-close-state-corruption": range(342, 347),
    "pipeline-smoke-runtime-raft-network-snapshot-corruption": range(347, 352),
}

CORRUPT_EXPECTATION_RE = re.compile(r"corrupt_[a-z_]+_expectation")


def _family_for_seed(seed: int) -> str | None:
    for family, rng in PIPELINE_SMOKE_RANGES.items():
        if seed in rng:
            return family
    return None


def audit_pipeline_smoke(argv: list[str]) -> int:
    """Every `corrupt_*_expectation` field must live in a pipeline-smoke family."""
    argparse.ArgumentParser(prog="dst pipeline-smoke").parse_args(argv)

    violations: list[str] = []
    for path in (FAILURE_CORPUS, SCHEDULE_CORPUS):
        if not path.exists():
            continue
        records = load_json(path)
        if not isinstance(records, list):
            print(f"ERROR: {rel(path)} root is not a list", file=sys.stderr)
            return 1
        for record in records:
            seed = record.get("seed") if isinstance(record, dict) else None
            try:
                seed_int = int(seed)
            except (TypeError, ValueError):
                continue
            serialized = json.dumps(record)
            matches = sorted(set(CORRUPT_EXPECTATION_RE.findall(serialized)))
            if not matches:
                continue
            if _family_for_seed(seed_int) is None:
                violations.append(
                    f"  {rel(path)}  seed={seed_int}  fields={matches}  "
                    f"family=<not in any pipeline-smoke range>"
                )

    if violations:
        print("ERROR: corrupt_*_expectation fields found outside pipeline-smoke families:")
        for line in violations:
            print(line)
        print()
        print(
            "Fix: extend PIPELINE_SMOKE_RANGES in audits.py (and rename the matching\n"
            "family in crates/ursula-sim/src/bin/ursula-sim/smoke.rs to pipeline-smoke-...)."
        )
        return 1

    total = sum(len(r) for r in PIPELINE_SMOKE_RANGES.values())
    print(
        f"OK: every corrupt_*_expectation seed in the corpus falls inside one of\n"
        f"    {len(PIPELINE_SMOKE_RANGES)} pipeline-smoke families ({total} seeds total)."
    )
    return 0


# =============================================================================
# 3. layer2
# =============================================================================

def audit_layer2(argv: list[str]) -> int:
    """No `fail_point!` macros. Simulator faults replace directed failpoints."""
    argparse.ArgumentParser(prog="dst layer2").parse_args(argv)

    try:
        out = subprocess.check_output(
            ["grep", "-rn", "fail_point!", "crates/"], cwd=ROOT, text=True,
        )
    except subprocess.CalledProcessError as exc:
        if exc.returncode != 1:
            raise
        out = ""
    hits = [line for line in out.splitlines() if line.strip()]

    if hits:
        print("ERROR: `fail_point!` macros found; inject the fault through the simulator instead:",
              file=sys.stderr)
        for line in hits:
            print(f"  {line}", file=sys.stderr)
        return 1

    print("OK: no `fail_point!` macros in crates/.")
    return 0


# =============================================================================
# 4. ci-shape
# =============================================================================

MULTI_ELEMENT_SHAPE_RE = re.compile(
    r'\[\s*\.\w+\.schedule\.fault_plan\.steps\[\]\.action\.action\s*\]'
    r'\s*==\s*\[[^\]]*,[^\]]*\]',
    re.MULTILINE,
)


SIM_TEST_FN_RE = re.compile(
    r"#\[test\]\s*(?P<attrs>(?:#\[[^\]]*\]\s*)*)(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(?P<name>\w+)",
)
SIM_CARGO_TEST_RE = re.compile(r"cargo test -p ursula-sim\b(?P<args>[^\n]*)")


def _sim_test_filters(path: Path) -> set[str]:
    """The libtest filters of every `cargo test -p ursula-sim` command in
    `path`, with `\\` line continuations joined."""
    text = re.sub(r"\\\n\s*", " ", path.read_text())
    filters: set[str] = set()
    for m in SIM_CARGO_TEST_RE.finditer(text):
        filters.update(
            token for token in m.group("args").split()
            if not token.startswith("-") and token != "\\"
        )
    return filters


def _unwired_replay_tests() -> list[str]:
    """Every non-ignored `*_replays` test in the simulation harness that no
    `cargo test -p ursula-sim` filter of the PR workflow matches. libtest
    matches a filter against the test's path, so the file stem counts."""
    filters = _sim_test_filters(PR_WORKFLOW)
    unwired = []
    for path in sorted(HARNESS_ROOT.rglob("*.rs")):
        for m in SIM_TEST_FN_RE.finditer(path.read_text()):
            name = m.group("name")
            if not name.endswith("_replays") or "#[ignore" in m.group("attrs"):
                continue
            test_path = f"{path.stem}::{name}"
            if not any(f in test_path for f in filters):
                unwired.append(f"  {rel(path)}: {name}")
    return unwired


def audit_ci_shape(argv: list[str]) -> int:
    """CI workflows must not contain multi-element strict-equality
    jq assertions on SimFaultAction discriminants, and the PR workflow must
    run every non-ignored `*_replays` simulation test."""
    argparse.ArgumentParser(prog="dst ci-shape").parse_args(argv)

    unwired = _unwired_replay_tests()
    if unwired:
        print("ERROR: `*_replays` tests no PR workflow filter runs:", file=sys.stderr)
        for line in unwired:
            print(line, file=sys.stderr)
        print(
            "Fix: keep the `_replays` filter in the PR DST job, or name the test so it matches.",
            file=sys.stderr,
        )
        return 1

    violations: list[str] = []
    for path in (PR_WORKFLOW, NIGHTLY_WORKFLOW):
        if not path.exists():
            continue
        text = path.read_text()
        for m in MULTI_ELEMENT_SHAPE_RE.finditer(text):
            line_no = text.count("\n", 0, m.start()) + 1
            snippet = m.group(0).replace("\n", " ").strip()
            if len(snippet) > 110:
                snippet = snippet[:107] + "..."
            violations.append(f"  {rel(path)}:{line_no}  {snippet}")

    if violations:
        print("ERROR: multi-element strict-equality step assertions found in CI workflows:",
              file=sys.stderr)
        for line in violations:
            print(line, file=sys.stderr)
        print(file=sys.stderr)
        print(
            "Fix: replace each clause with `cargo run -p ursula-sim --bin\n"
            "ursula-sim -- assert-shape --artifact PATH --steps-exact V1,V2,...`.\n"
            "That subcommand uses matches! on SimFaultAction so renames are compile errors.",
            file=sys.stderr,
        )
        return 1

    print("OK: no multi-element strict-equality step-shape jq assertions found in CI workflows.")
    print("OK: the PR workflow runs every `*_replays` simulation test.")
    return 0


# =============================================================================
# 5. modularity
# =============================================================================

HARNESS_MOD = HARNESS_ROOT / "mod.rs"

# Ratchet history:
#   12_500 — initial scaffold
#   11_000 — extracted #[cfg(test)] mod tests into madsim_harness/tests.rs
#   10_300 — extracted SimTrace + SimEvent into madsim_harness/trace.rs
#    9_600 — extracted SimSchedule::generate_* into madsim_harness/generators.rs
#    8_600 — extracted run_cold_*_inner scenarios into madsim_harness/cold_path.rs
#    7_000 — extracted run_http_*_inner scenarios into madsim_harness/http.rs
#    4_700 — extracted run_runtime_*_inner scenarios into runtime_scenarios.rs
#    4_000 — extracted Raft scenarios into raft_scenarios.rs
#    3_400 — extracted fault types into faults_inner.rs
#    2_400 — extracted ThreeNodeRaftSim dispatch + introspect helpers
#    2_500 — re-baseline (upward, one-time): nightly rustfmt imports_granularity="Item"
#            split grouped `use` blocks one-per-line, inflating mod.rs 2367 -> 2445
#            (+78 net lines, all imports, no logic). Aligned the ratchet with the
#            target so a global formatting policy isn't mistaken for logic creep.
LINE_BUDGET = 2_500
TARGET_FINAL_BUDGET = 2_500


def audit_modularity(argv: list[str]) -> int:
    """Ratcheting line budget on madsim_harness/mod.rs.

    Scenario code lives in the sibling modules of madsim_harness/. This audit
    keeps mod.rs from growing back into a monolith.
    """
    argparse.ArgumentParser(prog="dst modularity").parse_args(argv)

    errors: list[str] = []
    actual_lines = 0
    if HARNESS_MOD.exists():
        actual_lines = sum(1 for _ in HARNESS_MOD.read_text().splitlines())
        if actual_lines > LINE_BUDGET:
            errors.append(
                f"{rel(HARNESS_MOD)} has {actual_lines} lines, exceeds LINE_BUDGET "
                f"{LINE_BUDGET} (target {TARGET_FINAL_BUDGET}). Migrate code "
                "into sub-mods or update LINE_BUDGET in audits.py downward."
            )

    if errors:
        print("DST harness modularity audit failed:", file=sys.stderr)
        for e in errors:
            print(f"- {e}", file=sys.stderr)
        return 1

    print(
        f"OK: madsim_harness/mod.rs at {actual_lines} lines "
        f"(budget {LINE_BUDGET}, target {TARGET_FINAL_BUDGET})."
    )
    return 0


# =============================================================================
# 6. seed-inventory: family discipline and seed budget
# =============================================================================

EXPECTED_PR_FAMILIES = {
    "pipeline-smoke-http-live-waiter-corruption",
    "pipeline-smoke-http-live-sse-corruption",
    "pipeline-smoke-http-producer-retry-corruption",
    "pipeline-smoke-http-snapshot-body-corruption",
    "http-protocol-surface-randomized",
    "pipeline-smoke-http-protocol-surface-randomized-backpressure-corruption",
    "pipeline-smoke-http-protocol-surface-randomized-corruption",
    "pipeline-smoke-http-protocol-surface-randomized-sse-corruption",
    "runtime-interleaving",
    "runtime-interleaving-write-failures",
    "pipeline-smoke-runtime-raft-network-close-state-corruption",
    "runtime-raft-network-cold-live-truncate-failures",
    "runtime-raft-network-cold-live-write-failures",
    "runtime-raft-network-cold-live-write-recovery",
    "runtime-raft-network-leader-failover-cold-live-read-failures",
    "pipeline-smoke-runtime-raft-network-leader-failover-read-corruption",
    "pipeline-smoke-runtime-raft-network-partial-read-corruption",
    "runtime-raft-network-randomized-cold-read-failures",
    "pipeline-smoke-runtime-raft-network-randomized-read-corruption",
    "pipeline-smoke-runtime-raft-network-snapshot-corruption",
    "pipeline-smoke-runtime-raft-network-tail-read-corruption",
    "runtime-raft-snapshot-install-failures",
    "leader-read-linearizability",
    "producer-repeats-exactly-once",
    "idle-expiry-cannot-reopen-an-epoch",
    "history-rewind-under-leader-churn",
    "stale-writers-after-restore",
    "recovery-plan-cut-under-restarts",
    "recovery-plan-survivor-campaigns",
    "routing-before-body-under-leader-churn",
    "marked-chunks-never-contradict",
    "recovery-plan-membership-rollback",
    "deleted-data-leaves-within-bound",
    "deleted-data-across-segments",
}
EXPECTED_PR_RANGES = {"60..=64", "137..=140"}
EXPECTED_NIGHTLY_FAMILIES = EXPECTED_PR_FAMILIES | {
    "runtime-raft-network-randomized-extended",
    "leader-read-linearizability-extended",
    "producer-repeats-exactly-once-extended",
    "history-rewind-under-leader-churn-extended",
    "recovery-plan-cut-under-restarts-extended",
    "recovery-plan-survivor-campaigns-extended",
    "routing-before-body-under-leader-churn-extended",
    "marked-chunks-never-contradict-extended",
    "recovery-plan-membership-rollback-extended",
    "deleted-data-leaves-within-bound-extended",
    "deleted-data-across-segments-extended",
}
EXPECTED_NIGHTLY_RANGES = {"60..=199", "503..=504"}

# PR ≤ 2 min at the measured ~0.3 s/seed of the bundled PR sweep on the CI
# runner; nightly ≤ 30 min at ~70 seeds/min/core conservative. See the `CI`
# section of docs/architecture/deterministic-simulation-testing.md.
PR_SEED_BUDGET = 240
NIGHTLY_SEED_BUDGET = 1500


def _supported_families() -> set[str]:
    # Parses the `SEED_FAMILIES` table in
    # crates/ursula-sim/src/bin/ursula-sim/smoke.rs.
    return set(re.findall(r'name:\s*"([a-z0-9-]+)"', SMOKE_RS.read_text()))


def _workflow_families(path: Path) -> set[str]:
    return set(re.findall(r"--seed-family\s+([a-z0-9-]+)", path.read_text()))


def _workflow_ranges(path: Path) -> set[str]:
    return set(re.findall(r"--seed-range\s+([0-9]+\.\.=[0-9]+)", path.read_text()))


def _family_seed_count(family: str, smoke: str) -> int | None:
    m = re.search(
        rf'name:\s*"{re.escape(family)}",\s*start:\s*(\d+),\s*end:\s*(\d+)', smoke,
    )
    if not m:
        return None
    return int(m.group(2)) - int(m.group(1)) + 1


def _total_seeds(workflow: Path, smoke: str) -> int:
    text = workflow.read_text()
    total = 0
    for lo, hi in re.findall(r"--seed-range\s+(\d+)\.\.=(\d+)", text):
        total += int(hi) - int(lo) + 1
    for family in _workflow_families(workflow):
        n = _family_seed_count(family, smoke)
        if n is not None:
            total += n
    return total


def _check_expected_families(
    label: str,
    actual_families: set[str],
    actual_ranges: set[str],
    expected_families: set[str],
    expected_ranges: set[str],
    supported: set[str],
) -> list[str]:
    errors = []
    unsupported = actual_families - supported
    if unsupported:
        errors.append(f"{label}: unsupported families in workflow: {sorted(unsupported)}")
    missing_families = expected_families - actual_families
    if missing_families:
        errors.append(f"{label}: missing expected families: {sorted(missing_families)}")
    missing_ranges = expected_ranges - actual_ranges
    if missing_ranges:
        errors.append(f"{label}: missing expected ranges: {sorted(missing_ranges)}")
    return errors


def audit_seed_inventory(argv: list[str]) -> int:
    """Family discipline: PR/nightly workflows have the expected
    families/ranges, every named family resolves to a supported handler,
    and total seed count is within the per-track budget."""
    argparse.ArgumentParser(prog="dst seed-inventory").parse_args(argv)

    supported = _supported_families()
    pr_families = _workflow_families(PR_WORKFLOW)
    pr_ranges = _workflow_ranges(PR_WORKFLOW)
    nightly_families = _workflow_families(NIGHTLY_WORKFLOW)
    nightly_ranges = _workflow_ranges(NIGHTLY_WORKFLOW)

    errors: list[str] = []
    errors.extend(_check_expected_families(
        "PR", pr_families, pr_ranges, EXPECTED_PR_FAMILIES, EXPECTED_PR_RANGES, supported,
    ))
    errors.extend(_check_expected_families(
        "nightly", nightly_families, nightly_ranges,
        EXPECTED_NIGHTLY_FAMILIES, EXPECTED_NIGHTLY_RANGES, supported,
    ))

    smoke = SMOKE_RS.read_text()
    pr_total = _total_seeds(PR_WORKFLOW, smoke)
    nightly_total = _total_seeds(NIGHTLY_WORKFLOW, smoke)
    if pr_total > PR_SEED_BUDGET:
        errors.append(
            f"PR: total seed count {pr_total} exceeds budget {PR_SEED_BUDGET} "
            "(re-run `dst throughput` if the runner got faster)"
        )
    if nightly_total > NIGHTLY_SEED_BUDGET:
        errors.append(
            f"nightly: total seed count {nightly_total} exceeds budget {NIGHTLY_SEED_BUDGET} "
            "(re-run `dst throughput` if the runner got faster)"
        )

    if errors:
        print("DST seed inventory audit failed:", file=sys.stderr)
        for e in errors:
            print(f"- {e}", file=sys.stderr)
        return 1

    print("DST seed inventory audit passed.")
    print(f"PR families: {len(pr_families)}; ranges: {', '.join(sorted(pr_ranges))}")
    print(f"PR total seeds: {pr_total} (budget {PR_SEED_BUDGET})")
    print(f"nightly families: {len(nightly_families)}; ranges: {', '.join(sorted(nightly_ranges))}")
    print(f"nightly total seeds: {nightly_total} (budget {NIGHTLY_SEED_BUDGET})")
    return 0


# =============================================================================
# 7. failure-guards: every failure corpus entry has fresh CI coverage
# =============================================================================

def _expected_failure_corpus() -> list[tuple[int, str]]:
    records = load_json(FAILURE_CORPUS)
    if not isinstance(records, list):
        raise SystemExit(f"failure corpus root is not a list: {FAILURE_CORPUS}")
    out: list[tuple[int, str]] = []
    for record in records:
        try:
            out.append((int(record["seed"]), str(record["invariant"])))
        except (KeyError, TypeError, ValueError) as exc:
            raise SystemExit(f"invalid failure corpus record: {record!r}: {exc}") from exc
    return sorted(out)


def _has_minimize_guard(ci: str, seed: int, invariant: str) -> bool:
    return bool(re.search(
        rf"--artifact\s+\S*/seed-{seed}-failure\.json\s*\\\s*"
        rf"\n\s*--invariant\s+{re.escape(invariant)}\s*\\\s*"
        rf"\n\s*--output\s+\S*/seed-{seed}-minimized\.json",
        ci, re.MULTILINE,
    ))


def _has_trace_guard(ci: str, seed: int, invariant: str) -> bool:
    return bool(re.search(
        rf"assert_minimized_failure_trace\s+\S*/seed-{seed}-minimized\.json\s+"
        rf"{re.escape(invariant)}(?:\s|$)",
        ci, re.MULTILINE,
    ))


def _has_replay_guard(ci: str, seed: int, invariant: str) -> bool:
    return bool(re.search(
        rf"--artifact\s+\S*/seed-{seed}-minimized-failure\.json\s*\\\s*"
        rf"\n\s*--expect-invariant\s+{re.escape(invariant)}(?:\s|$)",
        ci, re.MULTILINE,
    ))


def audit_failure_guards(argv: list[str]) -> int:
    """Every failure-smoke corpus entry must have a fresh PR CI path that
    regenerates the failure, minimizes it, asserts the embedded trace,
    and replays the minimized artifact."""
    argparse.ArgumentParser(prog="dst failure-guards").parse_args(argv)

    ci = PR_WORKFLOW.read_text()
    missing: list[str] = []
    for seed, invariant in _expected_failure_corpus():
        if not _has_minimize_guard(ci, seed, invariant):
            missing.append(f"seed {seed} invariant {invariant}: missing minimize guard")
        if not _has_trace_guard(ci, seed, invariant):
            missing.append(f"seed {seed} invariant {invariant}: missing embedded trace guard")
        if not _has_replay_guard(ci, seed, invariant):
            missing.append(f"seed {seed} invariant {invariant}: missing replay guard")
    if missing:
        print("DST failure guard audit failed:", file=sys.stderr)
        for m in missing:
            print(f"- {m}", file=sys.stderr)
        return 1
    print("DST failure guard audit passed.")
    return 0


# =============================================================================
# meta: run every audit
# =============================================================================

# Crates vendored under `third_party/` because their published versions break
# madsim replay. A `[patch.crates-io]` entry whose version no longer matches
# the crate graph is dropped by cargo with only a warning, so check the lock.
SIM_PATCHES: dict[str, str] = {
    # `select!` branch shuffle, keyed to futures-util's version.
    "futures-macro": "futures-util",
}


def _lock_packages() -> list[dict[str, str]]:
    packages: list[dict[str, str]] = []
    for block in (ROOT / "Cargo.lock").read_text().split("[[package]]")[1:]:
        fields = dict(re.findall(r'^(name|version|source) = "([^"]*)"$', block, re.MULTILINE))
        packages.append(fields)
    return packages


def audit_sim_patches(argv: list[str]) -> int:
    """Vendored simulation patches are the versions cargo actually builds."""
    argparse.ArgumentParser(prog="dst sim-patches").parse_args(argv)
    packages = _lock_packages()
    problems: list[str] = []
    for patched, keyed_to in SIM_PATCHES.items():
        entries = [p for p in packages if p.get("name") == patched]
        if len(entries) != 1 or "source" in entries[0]:
            sources = [p.get("source", "path") for p in entries]
            problems.append(f"{patched}: expected one path package in Cargo.lock, found {sources}")
            continue
        anchors = {p["version"] for p in packages if p.get("name") == keyed_to}
        if anchors != {entries[0]["version"]}:
            problems.append(
                f"{patched} {entries[0]['version']} does not match {keyed_to} {sorted(anchors)}; "
                f"update third_party/{patched} to the new release"
            )
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    if problems:
        return 1
    print(f"OK: {len(SIM_PATCHES)} simulation patch(es) applied.")
    return 0


AUDITS: dict[str, callable] = {
    "nondeterminism": audit_nondeterminism,
    "pipeline-smoke": audit_pipeline_smoke,
    "layer2": audit_layer2,
    "ci-shape": audit_ci_shape,
    "modularity": audit_modularity,
    "seed-inventory": audit_seed_inventory,
    "failure-guards": audit_failure_guards,
    "sim-patches": audit_sim_patches,
}


def audit_all(argv: list[str]) -> int:
    """Run every audit; return non-zero if any fails."""
    argparse.ArgumentParser(prog="dst all").parse_args(argv)
    failures = []
    for name, fn in AUDITS.items():
        print(f"--- {name} ---")
        rc = fn([])
        if rc != 0:
            failures.append(name)
        print()
    if failures:
        print(f"FAIL: {len(failures)}/{len(AUDITS)} audit(s) failed: {', '.join(failures)}",
              file=sys.stderr)
        return 1
    print(f"OK: all {len(AUDITS)} audits passed.")
    return 0
