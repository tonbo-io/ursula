#!/usr/bin/env python3
"""Tests of check_patch_api.py: the baseline rule and the surface parsers."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from pathlib import PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_patch_api as api  # noqa: E402


def reader(files: dict[str, str]):
    def read(path: PurePosixPath) -> str | None:
        return files.get(str(path))

    def list_files(directory: PurePosixPath) -> list[PurePosixPath]:
        return [PurePosixPath(name) for name in files if name.startswith(f"{directory}/")]

    return read, list_files


def npm_tree(exports: dict, sources: dict[str, str]) -> dict[str, str]:
    package = {"name": "@scope/pkg", "exports": exports}
    files = {f"{api.NPM_PACKAGE}/package.json": json.dumps(package)}
    files.update({f"{api.NPM_PACKAGE}/src/{name}": text for name, text in sources.items()})
    return files


class BaselineTest(unittest.TestCase):
    TAGS = ["v0.10.1", "v0.11.0", "v0.11.4", "v0.11.5", "v0.11.6-rc.1", "v0.12.0-alpha.1"]

    def test_a_patch_compares_with_the_newest_release_of_its_line_at_or_below_it(self):
        self.assertEqual(api.baseline_tag("0.11.6", self.TAGS), "v0.11.5")
        self.assertEqual(api.baseline_tag("0.11.5", self.TAGS), "v0.11.5")
        self.assertEqual(api.baseline_tag("0.11.4", self.TAGS), "v0.11.4")

    def test_a_line_without_a_release_has_no_baseline(self):
        self.assertIsNone(api.baseline_tag("0.12.0", self.TAGS))
        self.assertIsNone(api.baseline_tag("1.0.0", self.TAGS))


class TypeScriptTest(unittest.TestCase):
    def test_reads_every_export_form_and_follows_star_exports(self):
        read, _ = reader(
            npm_tree(
                {".": {"types": "./dist/index.d.ts"}},
                {
                    "index.ts": """
                        // export const commented = 1;
                        /* export function alsoCommented() {} */
                        const url = "http://example.com"; // a string with //
                        export { a, b as renamed, type T } from "./a.ts";
                        export type { U } from "./a.js";
                        export {
                          multi,
                          type Line,
                        } from "./a.ts";
                        export * from "./star.ts";
                        export * as ns from "./a.ts";
                        export async function open() {}
                        export declare abstract class Base {}
                        export interface Options {}
                        export type Alias = string;
                        export const enum Kind { A }
                        export default open;
                    """,
                    "star.ts": "export function fromStar() {}\nexport * from './index.ts';",
                },
            )
        )
        names = api.ts_exports(read, api.NPM_PACKAGE / "src/index.ts")
        self.assertEqual(
            names,
            {
                "a", "renamed", "T", "U", "multi", "Line", "fromStar", "ns", "open",
                "Base", "Options", "Alias", "Kind", "default",
            },
        )

    def test_a_removed_subpath_and_a_removed_name_are_removals(self):
        """The 0.11.3 case: a patch dropped the `./tonbo` subpath."""
        exports = {
            ".": {"types": "./dist/index.d.ts"},
            "./tonbo": {"types": "./dist/tonbo.d.ts", "default": "./dist/tonbo.js"},
        }
        before = api.npm_surface(
            reader(npm_tree(exports, {"index.ts": "export function a() {}\nexport function b() {}",
                                      "tonbo.ts": "export function t() {}"}))[0]
        )
        after = api.npm_surface(
            reader(npm_tree({".": exports["."]}, {"index.ts": "export function a() {}"}))[0]
        )
        self.assertEqual(api.removals(before, after), ["@scope/pkg: b", "@scope/pkg/tonbo"])
        self.assertEqual(api.removals(after, before), [])


class PythonTest(unittest.TestCase):
    def test_all_wins_and_private_modules_are_not_api(self):
        source = api.PYTHON_SOURCE / "pkg"
        read, list_files = reader(
            {
                f"{source}/__init__.py": "from ._impl import open\n__all__ = ['open', 'Error']",
                f"{source}/errors.py": "import os\nfrom x import Y as Z\nclass Error: ...\n"
                "def _hidden(): ...\nLIMIT: int = 3\n",
                f"{source}/_impl.py": "def open(): ...",
                f"{source}/__main__.py": "print()",
                f"{api.PYTHON_PACKAGE}/pyproject.toml": "[project.scripts]\npkg = 'pkg:main'\n",
            }
        )
        self.assertEqual(
            api.python_surface(read, list_files),
            {
                "pkg": ["Error", "open"],
                "pkg.errors": ["Error", "LIMIT", "Z", "os"],
                "script pkg": [],
            },
        )


if __name__ == "__main__":
    unittest.main()
