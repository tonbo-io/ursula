#!/usr/bin/env python3
import subprocess
import unittest


def render_chart(*values: str) -> str:
    return subprocess.check_output(
        ["helm", "template", "test", "charts/ursula-chaos", *values], text=True
    )


class ChaosHelmTemplateTest(unittest.TestCase):
    def test_renders_record_stream_workload_and_scoped_rbac(self) -> None:
        rendered = render_chart(
            "--set",
            "statusS3Uri=s3://status-bucket/chaos/status.json",
        )

        self.assertIn("kind: Role\nmetadata:\n  name: test-ursula-chaos", rendered)
        self.assertIn('resourceNames:\n      - "ursula-0"', rendered)
        self.assertIn("verbs: [\"get\", \"delete\"]", rendered)
        self.assertIn("- --fault-backend=kubernetes", rendered)
        self.assertIn("- --record-stream-count=8", rendered)
        self.assertIn(
            "- --node=ursula-0=ursula-0=http://ursula-0.ursula-headless.default.svc.cluster.local:4437",
            rendered,
        )

    def test_can_target_ursula_in_another_namespace(self) -> None:
        rendered = render_chart(
            "--namespace",
            "chaos-system",
            "--set",
            "target.namespace=ursula",
        )

        self.assertIn("kind: Role\nmetadata:\n  name: test-ursula-chaos\n  namespace: ursula", rendered)
        self.assertIn("namespace: chaos-system\nroleRef:", rendered)
        self.assertIn("ursula-headless.ursula.svc.cluster.local", rendered)


if __name__ == "__main__":
    unittest.main()
