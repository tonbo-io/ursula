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


def deployment_contract_values() -> tuple[str, ...]:
    return (
        "--namespace",
        "ursula",
        "--set",
        "deploymentContract.expectedNamespace=ursula",
        "--set",
        "deploymentContract.serverServiceAccountName=ursula-storage",
        "--set",
        "deploymentContract.serverRoleArn=arn:aws:iam::123456789012:role/server",
        "--set",
        "deploymentContract.serverS3Prefix=server-data",
        "--set",
        "deploymentContract.indexerServiceAccountName=ursula-indexer",
        "--set",
        "deploymentContract.indexerRoleArn=arn:aws:iam::123456789012:role/indexer",
        "--set",
        "deploymentContract.indexerS3Prefix=index-data",
        "--set",
        "serviceAccount.name=ursula-storage",
        "--set",
        "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::123456789012:role/server",
        "--set",
        "s3.prefix=server-data",
        "--set",
        "indexer.serviceAccount.name=ursula-indexer",
        "--set",
        "indexer.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::123456789012:role/indexer",
        "--set",
        "indexer.s3.prefix=index-data",
    )


def indexer_values() -> tuple[str, ...]:
    return (
        "--set",
        "s3.bucket=index-bucket",
        "--set",
        "indexer.enabled=true",
    )


def hook_annotations(rendered: str) -> dict[tuple[str, str], dict[str, str]]:
    """Map every rendered Helm hook to its own ``helm.sh/*`` annotations.

    Keyed by ``(kind, metadata.name)`` so a caller asserts per resource. A
    substring search over the whole render cannot do that: it passes as soon as
    any single resource carries the expected annotation, which is how a hook
    group can be half-configured and still look green.

    Stdlib only, deliberately. CI runs this file with a bare ``python3`` and
    installs nothing, so a YAML parser is not available to assume.
    """
    resources: dict[tuple[str, str], dict[str, str]] = {}
    for document in re.split(r"^---$", rendered, flags=re.M):
        kind = re.search(r"^kind: (?P<kind>\S+)$", document, re.M)
        metadata = re.search(r"^metadata:\n(?P<block>(?:[ \t].*\n?)*)", document, re.M)
        if not kind or not metadata:
            continue
        block = metadata.group("block")
        annotations = dict(re.findall(r'^ {4}"(helm\.sh/[^"]+)": (.+)$', block, re.M))
        if "helm.sh/hook" not in annotations:
            continue
        name = re.search(r"^ {2}name: (?P<name>\S+)$", block, re.M)
        resources[(kind.group("kind"), name.group("name") if name else "")] = annotations
    return resources


class HelmTemplateConfigTest(unittest.TestCase):
    def test_matching_deployment_contract_renders(self) -> None:
        render_chart(*deployment_contract_values())

    def test_deployment_contract_rejects_namespace_drift(self) -> None:
        values = list(deployment_contract_values())
        values[1] = "another-namespace"

        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *values],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            'deploymentContract expected namespace "ursula", but Helm is rendering namespace "another-namespace"',
            result.stderr,
        )

    def test_deployment_contract_rejects_role_drift(self) -> None:
        values = list(deployment_contract_values())
        role_index = values.index("serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::123456789012:role/server")
        values[role_index] = "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::123456789012:role/wrong"

        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *values],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "deploymentContract serverRoleArn does not match serviceAccount.annotations[eks.amazonaws.com/role-arn]",
            result.stderr,
        )

    def test_server_update_strategy_defaults_to_rolling_update(self) -> None:
        rendered = render_chart("--set", "s3.bucket=bkt")

        self.assertIn(
            "podManagementPolicy: Parallel\n  updateStrategy:\n    type: RollingUpdate",
            rendered,
        )
        self.assertNotIn("app.kubernetes.io/component: ondelete-migration", rendered)

    def test_server_update_strategy_stages_on_delete_with_migration_hook(self) -> None:
        rendered = render_chart(
            "--set",
            "s3.bucket=bkt",
            "--set",
            "server.updateStrategy=OnDelete",
        )

        self.assertIn(
            "podManagementPolicy: Parallel\n  updateStrategy:\n    type: OnDelete\n  selector:",
            rendered,
        )
        self.assertIn("kind: Job\nmetadata:\n  name: test-ursula-ondelete-migration", rendered)

        # Per resource, not once across the whole render: the hook only cleans
        # up after itself if every object it creates carries the policy, and a
        # missing Role or ServiceAccount leaves the Job unable to run at all.
        migration = {
            (kind, name): annotations
            for (kind, name), annotations in hook_annotations(rendered).items()
            if name == "test-ursula-ondelete-migration"
        }
        self.assertEqual(
            {kind for kind, _ in migration},
            {"ServiceAccount", "Role", "RoleBinding", "Job"},
        )
        for (kind, name), annotations in sorted(migration.items()):
            with self.subTest(kind=kind, name=name):
                self.assertEqual(annotations["helm.sh/hook"], "pre-upgrade")
                self.assertEqual(
                    annotations["helm.sh/hook-delete-policy"],
                    "before-hook-creation,hook-succeeded,hook-failed",
                )
        self.assertIn(
            '- --patch={"spec":{"updateStrategy":{"rollingUpdate":null}}}',
            rendered,
        )
        self.assertIn("resourceNames:\n      - test-ursula", rendered)
        job_pod = re.search(
            r"kind: Job\n.*?template:\n    metadata:\n      labels:\n"
            r"(?P<labels>.*?)    spec:",
            rendered,
            re.S,
        )
        self.assertIsNotNone(job_pod)
        pod_labels = job_pod.group("labels")
        self.assertIn("app.kubernetes.io/component: ondelete-migration", pod_labels)
        self.assertNotIn("app.kubernetes.io/name:", pod_labels)
        self.assertNotIn("app.kubernetes.io/instance:", pod_labels)

    def test_gitops_can_disable_the_duplicate_on_delete_migration(self) -> None:
        rendered = render_chart(
            "--set",
            "s3.bucket=bkt",
            "--set",
            "server.updateStrategy=OnDelete",
            "--set",
            "server.onDeleteMigration.enabled=false",
        )

        self.assertIn("updateStrategy:\n    type: OnDelete", rendered)
        self.assertNotIn("app.kubernetes.io/component: ondelete-migration", rendered)

    def test_every_deployment_role_uses_the_unified_ursula_binary(self) -> None:
        rendered = render_chart(
            *indexer_values(),
            "--set",
            "gateway.enabled=true",
        )

        self.assertNotIn("/usr/local/bin/ursulagw", rendered)
        self.assertNotIn("/usr/local/bin/ursula-indexer", rendered)
        self.assertIn("- /usr/local/bin/ursula\n          args:\n            - gateway", rendered)
        self.assertIn("- /usr/local/bin/ursula\n          args:\n            - indexer", rendered)
        self.assertIn(
            'exec /usr/local/bin/ursula server --config "${config_path}"',
            rendered,
        )

    def test_gateway_can_prefer_same_zone_service_endpoints(self) -> None:
        rendered = render_chart(
            "--set",
            "s3.bucket=bkt",
            "--set",
            "gateway.service.trafficDistribution=PreferSameZone",
        )

        gateway_service = re.search(
            r"kind: Service\nmetadata:\n  name: test-ursula-gateway\n.*?"
            r"spec:\n(?P<spec>.*?)(?:\n---|\Z)",
            rendered,
            re.S,
        )
        self.assertIsNotNone(gateway_service)
        self.assertIn(
            'trafficDistribution: "PreferSameZone"',
            gateway_service.group("spec"),
        )

    def test_gateway_omits_empty_traffic_distribution(self) -> None:
        rendered = render_chart("--set", "s3.bucket=bkt")

        self.assertNotIn("trafficDistribution:", rendered)

    def test_admin_plane_defaults_to_loopback(self) -> None:
        config = render_config("--set", "s3.bucket=bkt")

        self.assertEqual(tomllib.loads(config)["server"]["admin_listen"], "127.0.0.1:4438")

    def test_admin_plane_can_bind_for_trusted_in_cluster_operator(self) -> None:
        config = render_config(
            "--set",
            "s3.bucket=bkt",
            "--set",
            "server.adminListen=0.0.0.0:4438",
        )

        self.assertEqual(tomllib.loads(config)["server"]["admin_listen"], "0.0.0.0:4438")

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
