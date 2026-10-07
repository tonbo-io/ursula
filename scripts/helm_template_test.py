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
    def test_rollout_target_starts_maintenance_drained(self) -> None:
        rendered = render_chart("--set", "s3.bucket=bkt")

        self.assertIn(
            'if [ -r "${rollout_state_dir}/phase" ] && [ -r "${rollout_state_dir}/node-id" ]; then',
            rendered,
        )
        self.assertIn("restarting|upgrading-restart-quiesce)", rendered)
        self.assertIn('if [ "${rollout_node_id}" = "${node_id}" ]; then', rendered)
        self.assertIn(
            'export URSULA_START_MAINTENANCE_DRAINED="${start_maintenance_drained}"',
            rendered,
        )
        self.assertNotIn("start_maintenance_drained =", render_config("--set", "s3.bucket=bkt"))
        self.assertIn(
            "- name: rollout-state\n              mountPath: /var/run/ursula-rollout-state\n              readOnly: true",
            rendered,
        )
        self.assertIn(
            "- name: rollout-state\n          configMap:\n            name: test-ursula-rollout-state\n            optional: true",
            rendered,
        )

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

    def test_graceful_rollout_hook_does_not_match_the_server_pdb(self) -> None:
        rendered = render_chart(
            "--set",
            "s3.bucket=bkt",
            "--set",
            "server.updateStrategy=OnDelete",
            "--set",
            "server.gracefulRollout.enabled=true",
        )

        rollout = {
            (kind, name): annotations
            for (kind, name), annotations in hook_annotations(rendered).items()
            if name == "test-ursula-graceful-rollout"
        }
        self.assertEqual(
            {kind for kind, _ in rollout},
            {"ServiceAccount", "Role", "RoleBinding", "ConfigMap", "Job"},
        )
        for (kind, name), annotations in sorted(rollout.items()):
            with self.subTest(kind=kind, name=name):
                expected_delete_policy = (
                    "before-hook-creation,hook-succeeded"
                    if kind == "Job"
                    else "before-hook-creation,hook-succeeded,hook-failed"
                )
                self.assertEqual(
                    annotations["helm.sh/hook-delete-policy"],
                    expected_delete_policy,
                )

        job = re.search(
            r"kind: Job\nmetadata:\n  name: test-ursula-graceful-rollout\n"
            r".*?template:\n    metadata:\n      labels:\n"
            r"(?P<labels>.*?)    spec:",
            rendered,
            re.S,
        )
        self.assertIsNotNone(job)
        self.assertIn(
            '"argocd.argoproj.io/hook-delete-policy": BeforeHookCreation,HookSucceeded',
            job.group(0),
        )
        pod_labels = job.group("labels")
        self.assertIn("app.kubernetes.io/component: graceful-rollout", pod_labels)
        self.assertNotIn("app.kubernetes.io/name:", pod_labels)
        self.assertNotIn("app.kubernetes.io/instance:", pod_labels)

    def test_shared_maintenance_job_rbac_and_inventory(self) -> None:
        values = (
            "--namespace", "test", "--set", "s3.bucket=bkt", "--set",
            "server.updateStrategy=OnDelete", "--set", "server.gracefulRollout.enabled=true",
            "--set", "server.gracefulRollout.maintenanceReservation=true",
            "--set", "server.coreCount=2", "--set", "raft.groupCount=256",
        )
        rendered = render_chart(*values)
        self.assertIn("exec /bin/sh /opt/rollout/maintenance-rollout.sh", rendered)
        self.assertIn('name: CORE_COUNT\n              value: "2"', rendered)
        self.assertIn('name: EXPECTED_GROUPS\n              value: "256"', rendered)
        self.assertIn('resourceNames: ["test-ursula-maintenance"]\n    verbs: ["get", "update"]', rendered)
        cluster_role = re.search(r"kind: ClusterRole\n.*?(?=\n---)", rendered, re.S).group(0)
        self.assertIn('resources: ["nodes"]\n    verbs: ["get"]', cluster_role)
        self.assertIn('resourceNames: ["test"]', cluster_role)
        self.assertNotIn('"delete"', cluster_role)
        # Only the script ConfigMap is rendered: persistent state is deliberately
        # not a disposable hook or an ordinary Helm resource that resets data.
        names = re.findall(r"kind: ConfigMap\nmetadata:\n  name: ([^\n]+)", rendered)
        self.assertNotIn("test-ursula-maintenance", names)
        other = render_chart(*values, "--namespace", "other")
        other_role = re.search(r"kind: ClusterRole\nmetadata:\n  name: ([^\n]+)", other).group(1)
        this_role = re.search(r"kind: ClusterRole\nmetadata:\n  name: ([^\n]+)", rendered).group(1)
        self.assertNotEqual(this_role, other_role)
        for invalid in ("server.replicaCount=2", "server.gracefulRollout.expectedGroups=1"):
            with self.subTest(invalid=invalid):
                run = subprocess.run(["helm", "template", "test", "charts/ursula", *values,
                                      "--set", invalid], text=True, capture_output=True)
                self.assertNotEqual(run.returncode, 0)

    def test_startup_ownership_is_explicit_and_has_only_exact_store_mutation(self) -> None:
        values = ("--namespace", "test", "--set", "s3.bucket=bkt", "--set",
                  "server.updateStrategy=OnDelete", "--set", "server.gracefulRollout.enabled=true",
                  "--set", "server.gracefulRollout.maintenanceReservation=true",
                  "--set", "server.startupOwnership.enabled=true")
        rendered = render_chart(*values)
        self.assertIn('name: URSULA_STARTUP_RESERVATION\n              value: "true"', rendered)
        self.assertIn('fieldPath: metadata.uid', rendered)
        self.assertIn('mountPath: /var/run/ursula-startup\n              readOnly: true', rendered)
        self.assertIn('expirationSeconds: 600', rendered)
        self.assertIn('name: kube-root-ca.crt', rendered)
        self.assertIn('path: /sys/devices/virtual/dmi/id/board_asset_tag\n            type: File', rendered)
        self.assertIn('mountPath: /var/run/ursula-physical-instance\n              readOnly: true', rendered)
        startup_role = re.search(r'kind: Role\nmetadata:\n  name: test-ursula-startup\n(?P<body>.*?)(?=\n---)', rendered, re.S).group('body')
        self.assertIn('resourceNames: ["test-ursula-maintenance"]\n    verbs: ["get", "update"]', startup_role)
        self.assertNotIn('"delete"', startup_role)
        self.assertNotIn('"create"', startup_role)
        self.assertNotIn('"patch"', startup_role)
        self.assertNotIn('URSULA_STARTUP_RESERVATION', render_chart("--set", "s3.bucket=bkt"))
        for invalid in ("server.replicaCount=2", "server.updateStrategy=RollingUpdate",
                        "server.gracefulRollout.maintenanceReservation=false",
                        "serviceAccount.create=false,serviceAccount.name=default",
                        "server.extraEnv[0].name=URSULA_STARTUP_RESERVATION,server.extraEnv[0].value=false"):
            with self.subTest(invalid=invalid):
                result = subprocess.run(["helm", "template", "test", "charts/ursula", *values, "--set", invalid], text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0)

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

    def test_wal_disk_watermarks_and_http_readiness_are_rendered(self) -> None:
        rendered = render_chart("--set", "s3.bucket=bkt")
        config = render_config("--set", "s3.bucket=bkt")
        wal = tomllib.loads(config)["raft"]["wal"]

        self.assertEqual(wal["min_available_size"], "536870912")
        self.assertEqual(wal["resume_available_size"], "1073741824")
        self.assertFalse(wal["allow_volatile_multi_peer"])
        self.assertIn(
            "readinessProbe:\n            httpGet:\n              path: /__ursula/ready\n              port: client",
            rendered,
        )

    def test_wal_fsync_policy_is_rendered_and_validated(self) -> None:
        wal = tomllib.loads(render_config("--set", "s3.bucket=bkt"))["raft"]["wal"]
        self.assertEqual(wal["fsync"], "never")

        wal = tomllib.loads(render_config("--set", "s3.bucket=bkt", "--set", "raft.walFsync=always"))[
            "raft"
        ]["wal"]
        self.assertEqual(wal["fsync"], "always")

        # A memory WAL ignores the policy, including the default.
        wal = tomllib.loads(
            render_config(
                "--set",
                "s3.bucket=bkt",
                "--set",
                "raft.storageMode=memory",
                "--set",
                "raft.allowVolatileMultiPeer=true",
            )
        )["raft"]["wal"]
        self.assertEqual(wal["backend"], "memory")

        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "raft.walFsync=interval",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("walFsync", result.stderr)

    def test_multi_peer_memory_wal_requires_explicit_opt_in(self) -> None:
        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                "--set",
                "raft.storageMode=memory",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("raft.allowVolatileMultiPeer=true", result.stderr)

        config = render_config(
            "--set",
            "raft.storageMode=memory",
            "--set",
            "raft.allowVolatileMultiPeer=true",
        )
        self.assertTrue(tomllib.loads(config)["raft"]["wal"]["allow_volatile_multi_peer"])

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

    def test_cold_pressure_hot_bytes_are_rendered(self) -> None:
        config = render_config(
            "--set",
            "coldStorage.enabled=true",
            "--set",
            "coldStorage.flush.pressureHotBytes=33554432",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["cold"]["flush_pressure_hot_size"], "33554432")

    def test_snapshot_pressure_limits_are_rendered(self) -> None:
        config = render_config(
            "--set",
            "raft.snapshotPressureMaxGroupsPerTick=8",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["raft"]["snapshot_pressure_max_groups_per_tick"], 8)

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
        self.assertIn("- --segment-bytes\n            - \"33554432\"", rendered)
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
        self.assertRegex(
            result.stderr,
            r"(?:/indexer/workers/leaseMs|indexer\.workers\.leaseMs)",
        )


    def test_defaults_enable_compaction_and_follow_the_runtime_snapshot_default(self) -> None:
        parsed = tomllib.loads(render_config("--set", "coldStorage.enabled=true", "--set", "s3.bucket=bkt"))

        self.assertIs(parsed["storage"]["cold"]["compaction_enabled"], True)
        # "auto" is Ursula's own default: S3 snapshots with an S3 cold store,
        # inline otherwise. The chart no longer pins "inline".
        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "auto")
        self.assertEqual(parsed["storage"]["snapshot"]["s3_prefix"], "snapshots")

    def test_default_snapshot_backend_without_cold_storage_needs_no_bucket(self) -> None:
        parsed = tomllib.loads(render_config())

        self.assertEqual(parsed["storage"]["cold"]["backend"], "none")
        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "auto")
        self.assertNotIn("s3", parsed["storage"]["cold"])

    def test_explicit_inline_snapshots_and_disabled_compaction_still_render(self) -> None:
        parsed = tomllib.loads(
            render_config(
                "--set",
                "coldStorage.enabled=true",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "coldStorage.compaction.enabled=false",
                "--set",
                "snapshotStore.backend=inline",
            )
        )

        self.assertIs(parsed["storage"]["cold"]["compaction_enabled"], False)
        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "inline")
        self.assertNotIn("s3_prefix", parsed["storage"]["snapshot"])

    def test_compaction_defaults_on_when_values_omit_the_key(self) -> None:
        parsed = tomllib.loads(
            render_config(
                "--set",
                "coldStorage.enabled=true",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "coldStorage.compaction=null",
            )
        )

        self.assertIs(parsed["storage"]["cold"]["compaction_enabled"], True)


if __name__ == "__main__":
    unittest.main()
