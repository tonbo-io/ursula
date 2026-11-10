#!/usr/bin/env python3
"""Refuse a patch that removes public API of the npm or Python client.

A patch release of a line (x.y.z after x.y.0) must not break code written
against an earlier release of the line. The Rust crates are checked by
`cargo semver-checks`. This script checks what that tool cannot see: the
export subpaths of `@tonbo/sqlite-ursula` and the names each one exports, and
the public modules of `sqlite-ursula` (Python) and the names each one
exports, against the newest release of the line at or below the workspace
version.

    check_patch_api.py baseline   # the tag to compare with, or nothing
    check_patch_api.py check      # compare with it; exit 1 on a removal

A workspace version whose line has no release yet (a new minor) has no
baseline: a new minor may change its API. Start a breaking change by moving
the workspace to the next minor.

The TypeScript surface is read from the sources with a small parser that
knows the export forms the package uses (declarations, `export { .. }`,
`export type { .. }`, `export * from` and `export * as`). It reports names,
not signatures: a changed signature is for review to catch.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from pathlib import PurePosixPath

ROOT = Path(__file__).resolve().parent.parent
NPM_PACKAGE = PurePosixPath("clients/sqlite-ursula")
PYTHON_PACKAGE = PurePosixPath("clients/sqlite-ursula-python")
PYTHON_SOURCE = PYTHON_PACKAGE / "src"

Read = Callable[[PurePosixPath], str | None]
List = Callable[[PurePosixPath], list[PurePosixPath]]

TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$")


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout


def workspace_version(cargo_toml: str) -> str:
    return tomllib.loads(cargo_toml)["workspace"]["package"]["version"]


def baseline_tag(version: str, tags: list[str]) -> str | None:
    """The newest release tag of `version`'s line at or below it."""
    matched = VERSION.match(version)
    if not matched:
        raise ValueError(f"workspace version {version!r} is not x.y.z")
    major, minor, patch = (int(part) for part in matched.groups())
    candidates = []
    for tag in tags:
        tagged = TAG.match(tag.strip())
        if not tagged:
            continue
        x, y, z = (int(part) for part in tagged.groups())
        if (x, y) == (major, minor) and z <= patch:
            candidates.append((z, tag.strip()))
    return max(candidates)[1] if candidates else None


# TypeScript ------------------------------------------------------------------


def strip_comments(source: str) -> str:
    """`source` without comments; string and template literals are kept."""
    out = []
    i = 0
    n = len(source)
    while i < n:
        c = source[i]
        if c in "'\"`":
            j = i + 1
            while j < n and source[j] != c:
                j += 2 if source[j] == "\\" else 1
            out.append(source[i : j + 1])
            i = j + 1
        elif source.startswith("//", i):
            j = source.find("\n", i)
            i = n if j < 0 else j
        elif source.startswith("/*", i):
            j = source.find("*/", i + 2)
            i = n if j < 0 else j + 2
            out.append(" ")
        else:
            out.append(c)
            i += 1
    return "".join(out)


IDENT = r"[A-Za-z_$][\w$]*"
DECLARATION = re.compile(
    r"\bexport\s+(?:declare\s+)?(?:async\s+)?(?:abstract\s+)?"
    r"(?:function\s*\*?|class|const\s+enum|enum|const|let|var|interface|type|namespace)"
    rf"\s+({IDENT})"
)
NAMED = re.compile(r"\bexport\s+(?:type\s+)?\{([^}]*)\}(?:\s*from\s*[\"']([^\"']+)[\"'])?")
STAR_AS = re.compile(rf"\bexport\s+\*\s+as\s+({IDENT})\s+from\b")
STAR = re.compile(r"\bexport\s+\*\s+from\s*[\"']([^\"']+)[\"']")
DEFAULT = re.compile(r"\bexport\s+default\b")


def resolve_module(read: Read, importer: PurePosixPath, specifier: str) -> PurePosixPath | None:
    if not specifier.startswith("."):
        return None
    base = PurePosixPath(*(importer.parent / specifier).parts)
    stem = str(base)
    for suffix in (".ts", ".js", ".mts", ".mjs", ".tsx"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    for candidate in (f"{stem}.ts", f"{stem}.tsx", f"{stem}.mts", f"{stem}/index.ts"):
        path = PurePosixPath(candidate)
        if read(path) is not None:
            return path
    return None


def ts_exports(read: Read, module: PurePosixPath, seen: frozenset = frozenset()) -> set[str]:
    """The names `module` exports, following `export * from` local modules."""
    source = read(module)
    if source is None or module in seen:
        return set()
    seen = seen | {module}
    text = strip_comments(source)
    names = set(DECLARATION.findall(text)) | set(STAR_AS.findall(text))
    for items, _ in NAMED.findall(text):
        for item in items.split(","):
            item = re.sub(r"^\s*type\s+", "", item).strip()
            if item:
                names.add(re.split(r"\s+as\s+", item)[-1].strip())
    if DEFAULT.search(text):
        names.add("default")
    for specifier in STAR.findall(text):
        target = resolve_module(read, module, specifier)
        if target is not None:
            names |= ts_exports(read, target, seen)
    return names


def source_of_types(package: PurePosixPath, types: str) -> PurePosixPath:
    """The source a `types` entry is built from: `./dist/x.d.ts` is `src/x.ts`."""
    relative = PurePosixPath(types.removeprefix("./"))
    if relative.parts[:1] == ("dist",):
        relative = PurePosixPath("src", *relative.parts[1:])
    name = relative.name
    for suffix in (".d.ts", ".d.mts", ".js", ".mjs"):
        if name.endswith(suffix):
            name = name[: -len(suffix)] + ".ts"
            break
    return package / relative.with_name(name)


def npm_surface(read: Read) -> dict[str, list[str]]:
    manifest = read(NPM_PACKAGE / "package.json")
    if manifest is None:
        return {}
    package = json.loads(manifest)
    exports = package.get("exports") or {".": {"types": package.get("types", "")}}
    if isinstance(exports, str):
        exports = {".": {"types": package.get("types", "")}}
    surface = {}
    for subpath, entry in exports.items():
        types = entry.get("types") if isinstance(entry, dict) else None
        names = ts_exports(read, source_of_types(NPM_PACKAGE, types)) if types else set()
        surface[f"{package['name']}{subpath.removeprefix('.')}"] = sorted(names)
    for command in sorted(package.get("bin") or {}):
        surface[f"bin {command}"] = []
    return surface


# Python ----------------------------------------------------------------------


def python_names(source: str) -> set[str]:
    """`__all__` when the module declares it, otherwise its public top-level names."""
    tree = ast.parse(source)
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else []
        if any(isinstance(target, ast.Name) and target.id == "__all__" for target in targets):
            return {ast.literal_eval(element) for element in node.value.elts}
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names |= {target.id for target in node.targets if isinstance(target, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            if isinstance(node, ast.ImportFrom) and node.module == "__future__":
                continue
            names |= {(alias.asname or alias.name).split(".")[0] for alias in node.names}
    return {name for name in names if not name.startswith("_")}


def python_surface(read: Read, list_files: List) -> dict[str, list[str]]:
    surface = {}
    for path in list_files(PYTHON_SOURCE):
        if path.suffix != ".py":
            continue
        module = path.relative_to(PYTHON_SOURCE).with_suffix("")
        parts = list(module.parts)
        if parts[-1] == "__init__":
            parts.pop()
        if not parts or any(part.startswith("_") for part in parts):
            continue
        source = read(path)
        if source is not None:
            surface[".".join(parts)] = sorted(python_names(source))
    pyproject = read(PYTHON_PACKAGE / "pyproject.toml")
    if pyproject is not None:
        scripts = tomllib.loads(pyproject).get("project", {}).get("scripts", {})
        for command in sorted(scripts):
            surface[f"script {command}"] = []
    return surface


# Comparison ------------------------------------------------------------------


def removals(baseline: dict[str, list[str]], current: dict[str, list[str]]) -> list[str]:
    removed = []
    for entry, names in sorted(baseline.items()):
        if entry not in current:
            removed.append(entry)
            continue
        removed.extend(f"{entry}: {name}" for name in sorted(set(names) - set(current[entry])))
    return removed


def tree_reader(tag: str) -> tuple[Read, List]:
    def read(path: PurePosixPath) -> str | None:
        result = subprocess.run(
            ["git", "show", f"{tag}:{path}"], cwd=ROOT, capture_output=True, text=True
        )
        return result.stdout if result.returncode == 0 else None

    def list_files(directory: PurePosixPath) -> list[PurePosixPath]:
        listed = git("ls-tree", "-r", "--name-only", tag, "--", str(directory))
        return [PurePosixPath(line) for line in listed.splitlines() if line]

    return read, list_files


def worktree_reader() -> tuple[Read, List]:
    def read(path: PurePosixPath) -> str | None:
        file = ROOT / path
        return file.read_text() if file.is_file() else None

    def list_files(directory: PurePosixPath) -> list[PurePosixPath]:
        listed = git("ls-files", "--", str(directory))
        return [PurePosixPath(line) for line in listed.splitlines() if line]

    return read, list_files


def surface(read: Read, list_files: List) -> dict[str, list[str]]:
    return {**npm_surface(read), **python_surface(read, list_files)}


def current_baseline() -> str | None:
    version = workspace_version((ROOT / "Cargo.toml").read_text())
    return baseline_tag(version, git("tag", "--list", "v*").splitlines())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["baseline", "check"])
    parser.add_argument("--baseline", help="compare with this tag instead of the line's newest")
    args = parser.parse_args()
    baseline = args.baseline or current_baseline()
    if args.command == "baseline":
        print(baseline or "")
        return 0
    if baseline is None:
        print("no release of the workspace version's line yet: nothing to compare")
        return 0
    removed = removals(surface(*tree_reader(baseline)), surface(*worktree_reader()))
    if removed:
        print(f"public client API that {baseline} has and this tree removes:")
        for item in removed:
            print(f"  {item}")
        print(
            "A patch release must not remove public API. Restore it, or move the "
            "workspace to the next minor."
        )
        return 1
    print(f"the npm and Python client API of {baseline} is all still there")
    return 0


if __name__ == "__main__":
    sys.exit(main())
