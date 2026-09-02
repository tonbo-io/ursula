import unittest
from unittest.mock import call, patch

from crates_release import topological_order, validate
from crates_release import validate_internal_dependency_requirements


class TopologicalOrderTests(unittest.TestCase):
    def test_orders_internal_dependencies_before_consumers(self) -> None:
        packages = {
            "leaf": {"dependencies": []},
            "middle": {
                "dependencies": [{"name": "leaf", "kind": None}],
            },
            "binary": {
                "dependencies": [
                    {"name": "middle", "kind": None},
                    {"name": "test-only", "kind": "dev"},
                ],
            },
            "test-only": {"dependencies": []},
        }

        order = topological_order(packages)

        self.assertLess(order.index("leaf"), order.index("middle"))
        self.assertLess(order.index("middle"), order.index("binary"))

    def test_rejects_publication_cycles(self) -> None:
        packages = {
            "left": {"dependencies": [{"name": "right", "kind": None}]},
            "right": {"dependencies": [{"name": "left", "kind": None}]},
        }

        with self.assertRaisesRegex(RuntimeError, "publication dependency cycle"):
            topological_order(packages)

    def test_requires_exact_internal_release_versions(self) -> None:
        packages = {
            "leaf": {"dependencies": []},
            "consumer": {
                "dependencies": [
                    {"name": "leaf", "kind": None, "req": "=0.5.0-patch1"}
                ],
            },
        }

        validate_internal_dependency_requirements(packages, "0.5.0-patch1")

        packages["consumer"]["dependencies"][0]["req"] = "^0.5.0-patch1"
        with self.assertRaisesRegex(RuntimeError, "expected '=0.5.0-patch1'"):
            validate_internal_dependency_requirements(packages, "0.5.0-patch1")

    @patch("crates_release.run")
    def test_validation_only_publish_dry_runs_leaf_crates(self, run) -> None:
        packages = {
            "leaf": {"dependencies": []},
            "consumer": {"dependencies": [{"name": "leaf", "kind": None}]},
        }

        validate(["leaf", "consumer"], packages)

        self.assertEqual(
            run.call_args_list,
            [
                call(["cargo", "package", "--list", "--locked", "-p", "leaf"]),
                call(
                    [
                        "cargo",
                        "publish",
                        "--dry-run",
                        "--no-verify",
                        "--locked",
                        "-p",
                        "leaf",
                    ]
                ),
                call(["cargo", "package", "--list", "--locked", "-p", "consumer"]),
            ],
        )


if __name__ == "__main__":
    unittest.main()
