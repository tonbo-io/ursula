#!/usr/bin/env python3
import re
import subprocess
import tomllib
import unittest


def render_config(*values: str) -> str:
    rendered = subprocess.check_output(
        ["helm", "template", "test", "charts/ursula", *values],
        text=True,
    )
    match = re.search(r"cat > \"\$\{config_path\}\" <<EOF\n(?P<config>.*?)\n    EOF", rendered, re.S)
    if not match:
        raise AssertionError("could not find generated Ursula config in helm output")
    return match.group("config")


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


if __name__ == "__main__":
    unittest.main()
