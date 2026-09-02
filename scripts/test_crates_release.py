import unittest

from crates_release import topological_order


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


if __name__ == "__main__":
    unittest.main()
