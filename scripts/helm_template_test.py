#!/usr/bin/env python3
import re
import subprocess
import tomllib
import unittest


def render_config(*values: str) -> str:
    rendered = render_chart(*values)
    match = re.search(r"cat > \"\$\{config_path\}\" <<EOF\n(?P<config>.*?)\n    EOF", rendered, re.S)
    if not match:
        raise AssertionError("could not find generated Ursula config in helm output")
    return match.group("config")


def render_chart(*values: str) -> str:
    return subprocess.check_output(["helm", "template", "test", "charts/ursula", *values], text=True)


def indexer_values() -> tuple[str, ...]:
    return (
        "--set",
        "s3.bucket=index-bucket",
        "--set",
        "indexer.enabled=true",
    )


class HelmTemplateConfigTest(unittest.TestCase):
    def test_max_uncommitted_value_uses_single_raft_table(self) -> None:
        config = render_config("--set", "raft.maxUncommittedBytesPerGroup=8388608", "--set", "s3.bucket=bkt")

        raft_table_count = sum(line.strip() == "[raft]" for line in config.splitlines())
        self.assertEqual(raft_table_count, 1)
        parsed = tomllib.loads(config)
        self.assertEqual(parsed["raft"]["max_uncommitted_size_per_group"], "8388608")

    def test_max_uncommitted_zero_is_rendered(self) -> None:
        config = render_config("--set", "raft.maxUncommittedBytesPerGroup=0", "--set", "s3.bucket=bkt")
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["raft"]["max_uncommitted_size_per_group"], "0")

    def test_cold_max_hot_bytes_zero_is_rendered(self) -> None:
        config = render_config(
            "--set",
            "coldStorage.enabled=true",
            "--set",
            "coldStorage.flush.maxHotBytesPerGroup=0",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["cold"]["max_hot_size_per_group"], "0")

    def test_snapshot_s3_renders_complete_config(self) -> None:
        config = render_config("--set", "snapshotStore.backend=s3", "--set", "s3.bucket=bkt")
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "s3")
        self.assertEqual(parsed["storage"]["cold"]["s3"]["bucket"], "bkt")

    def test_snapshot_drive_interval_zero_is_rendered(self) -> None:
        config = render_config(
            "--set",
            "snapshotStore.driveIntervalMs=0",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["snapshot"]["drive_interval"], "0ms")

    def test_cold_cache_zero_can_disable_default_cache(self) -> None:
        config = render_config(
            "--set",
            "coldStorage.enabled=true",
            "--set",
            "coldStorage.cache.maxSizeBytes=0",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["cold"]["cache"]["max_size"], "0")

    def test_cold_cache_null_renders_no_cache_section(self) -> None:
        config = render_config(
            "--set",
            "coldStorage.enabled=true",
            "--set",
            "coldStorage.cache=null",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertNotIn("cache", parsed["storage"]["cold"])

    def test_indexer_renders_inherited_s3_and_health_probes(self) -> None:
        rendered = render_chart(*indexer_values())

        self.assertIn("- --s3-bucket\n            - \"index-bucket\"", rendered)
        self.assertIn("- --s3-prefix\n            - \"event-index\"", rendered)
        self.assertIn("- --segment-records\n            - \"4096\"", rendered)
        self.assertIn("- --worker-id\n            - $(POD_NAME)", rendered)
        self.assertIn("path: /livez", rendered)
        self.assertIn("path: /readyz", rendered)
        self.assertIn("name: test-ursula-indexer\n", rendered)

    def test_indexer_multiple_replicas_render_one_shared_worker_pool(self) -> None:
        rendered = render_chart(
            *indexer_values(),
            "--set",
            "indexer.replicaCount=3",
        )

        self.assertIn("replicas: 3", rendered)
        self.assertEqual(rendered.count("kind: Deployment\nmetadata:\n  name: test-ursula-indexer"), 1)

    def test_indexer_worker_pool_renders_pdb_and_spread(self) -> None:
        rendered = render_chart(
            *indexer_values(),
            "--set",
            "indexer.replicaCount=2",
        )

        self.assertIn("type: RollingUpdate", rendered)
        self.assertIn("topologyKey: topology.kubernetes.io/zone", rendered)
        self.assertIn("name: test-ursula-indexer\n", rendered)

    def test_indexer_rejects_invalid_worker_lease(self) -> None:
        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                *indexer_values(),
                "--set",
                "indexer.workers.leaseMs=0",
            ],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/indexer/workers/leaseMs", result.stderr)


if __name__ == "__main__":
    unittest.main()
