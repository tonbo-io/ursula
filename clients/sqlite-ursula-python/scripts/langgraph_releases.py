"""The LangGraph releases CI runs tests/test_langgraph.py on, from pyproject.toml.

    python3 scripts/langgraph_releases.py requirements oldest|newest
    python3 scripts/langgraph_releases.py release oldest|newest

``requirements`` prints the requirements of the ``langgraph-oldest`` or ``langgraph-newest``
dependency group, one per line. ``release`` prints the group's ``langgraph-checkpoint-sqlite``
release, which the tests check is the one installed. Both first check the groups against the
``langgraph`` extra's range, ``>=oldest,<first-unsupported``: the oldest group pins its lower
bound, and the newest group a release inside it.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = "langgraph-checkpoint-sqlite"
RANGE = re.compile(rf"^{PACKAGE}>=(\d+\.\d+\.\d+),<(\d+\.\d+(?:\.\d+)?)$")
PIN = re.compile(r"^([A-Za-z0-9-]+)==(\d+\.\d+\.\d+)$")


def version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def pinned(group: list[str]) -> dict[str, str]:
    pins = {}
    for requirement in group:
        found = PIN.fullmatch(requirement)
        if found is None:
            raise SystemExit(f"{requirement!r} is not a pin to one release")
        pins[found[1]] = found[2]
    if PACKAGE not in pins:
        raise SystemExit(f"the group pins no {PACKAGE}: {group}")
    return pins


def groups() -> dict[str, list[str]]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    (extra,) = project["project"]["optional-dependencies"]["langgraph"]
    bounds = RANGE.fullmatch(extra)
    if bounds is None:
        raise SystemExit(
            f"the langgraph extra is not {PACKAGE}>=oldest,<first-unsupported: {extra}"
        )
    lowest, unsupported = version(bounds[1]), version(bounds[2])
    found = {
        which: project["dependency-groups"][f"langgraph-{which}"] for which in ("oldest", "newest")
    }
    oldest = version(pinned(found["oldest"])[PACKAGE])
    newest = version(pinned(found["newest"])[PACKAGE])
    if oldest != lowest:
        raise SystemExit(f"langgraph-oldest pins {PACKAGE} {oldest}, not the extra's {lowest}")
    if not lowest <= newest < unsupported:
        raise SystemExit(f"langgraph-newest pins {PACKAGE} {newest}, outside the extra: {extra}")
    return found


def main() -> None:
    match sys.argv[1:]:
        case ["requirements", ("oldest" | "newest") as which]:
            print("\n".join(groups()[which]))
        case ["release", ("oldest" | "newest") as which]:
            print(pinned(groups()[which])[PACKAGE])
        case _:
            raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
