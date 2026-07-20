#!/usr/bin/env python3
import tomllib
import unittest
from collections.abc import Iterator
from contextlib import contextmanager

from ursula_ec2 import ClusterConfig
from ursula_ec2 import Ec2Ops
from ursula_ec2 import Node


@contextmanager
def ops(cold_env: dict[str, str]) -> Iterator[Ec2Ops]:
    config = ClusterConfig(
        nodes=[
            Node(
                id=1,
                name="node-1",
                instance_id="i-1",
                az="az-a",
                public_ip="1.1.1.1",
                private_ip="10.0.0.1",
            )
        ],
        client=None,
        ssh_user="ec2-user",
        port=4437,
        binary="/opt/ursula/ursula",
        pid_prefix="/tmp/ursula",
        log_prefix="/tmp/ursula",
        config_prefix="/tmp/ursula-config",
        core_count=2,
        raft_group_count=4,
        raft_memory=False,
        raft_log_prefix="/var/lib/ursula/raft-log",
        init_membership_per_group=True,
        cold_env=cold_env,
        perf_compare=None,
    )
    op = Ec2Ops(config, known_hosts="/tmp/known-hosts", verbose=False)
    try:
        yield op
    finally:
        op.close()


class Ec2ConfigMappingTest(unittest.TestCase):
    def test_known_legacy_keys_map_into_generated_toml_config(self) -> None:
        with ops(
            {
                "URSULA_COLD_BACKEND": "s3",
                "URSULA_COLD_S3_BUCKET": "bucket",
                "URSULA_COLD_GC_MAX_ENTRIES_PER_GROUP": "77",
                "URSULA_COLD_FLUSH_BYTES": "12MiB",
                "URSULA_S3_PROBE_TIMEOUT_MS": "2500",
                "URSULA_S3_PROBE_UNHEALTHY_TICKS": "4",
                "URSULA_S3_PROBE_HEAL_TICKS": "6",
                "URSULA_SNAPSHOT_BACKEND": "s3",
                "URSULA_SNAPSHOT_DRIVE_INTERVAL_MS": "0",
            }
        ) as op:
            generated = op.generate_config(op.config.nodes[0])
        config = tomllib.loads(generated)

        self.assertEqual(config["storage"]["cold"]["gc_max_entries"], 77)
        self.assertEqual(config["storage"]["cold"]["flush_size"], "12MiB")
        self.assertEqual(config["storage"]["cold"]["s3"]["probe_timeout"], "2500ms")
        self.assertEqual(config["storage"]["cold"]["s3"]["unhealthy_ticks"], 4)
        self.assertEqual(config["storage"]["cold"]["s3"]["heal_ticks"], 6)
        self.assertEqual(config["storage"]["snapshot"]["drive_interval"], "0ms")

    def test_unknown_keys_pass_through_as_environment(self) -> None:
        with ops(
            {
                "RUST_LOG": "ursula=info,ursula_runtime=debug",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
                "URSULA_TOKIO_CONSOLE": "1",
            }
        ) as op:
            env = op.environment_passthrough()

        self.assertEqual(env["RUST_LOG"], "ursula=info,ursula_runtime=debug")
        self.assertEqual(env["OTEL_EXPORTER_OTLP_ENDPOINT"], "http://collector:4317")
        self.assertEqual(env["URSULA_TOKIO_CONSOLE"], "1")

    def test_known_mapped_keys_do_not_pass_through_as_environment(self) -> None:
        with ops({"URSULA_COLD_S3_BUCKET": "bucket"}) as op:
            env = op.environment_passthrough()

        self.assertNotIn("URSULA_COLD_S3_BUCKET", env)

    def test_systemd_unit_contains_environment_lines(self) -> None:
        with ops({"RUST_LOG": "ursula=info"}) as op:
            unit = op.systemd_unit(op.config.nodes[0], "always")

        self.assertIn('Environment="RUST_LOG=ursula=info"', unit)

    def test_non_systemd_start_command_includes_environment_pass_through(self) -> None:
        with ops(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
                "RUST_LOG": "ursula=info",
            }
        ) as op:
            node = op.config.nodes[0]
            captured: dict[str, object] = {}

            def fake_ssh(ssh_node: Node, command: str) -> None:
                captured["node"] = ssh_node
                captured["command"] = command

            op.ssh = fake_ssh
            op.start_node(node)

        command = captured["command"]
        self.assertIs(captured["node"], node)
        self.assertIsInstance(command, str)
        self.assertIn(
            "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317 RUST_LOG=ursula=info /opt/ursula/ursula",
            command,
        )


if __name__ == "__main__":
    unittest.main()
