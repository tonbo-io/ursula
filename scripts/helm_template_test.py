#!/usr/bin/env python3
import json
import re
import subprocess
import tomllib
import unittest


# Every install names its deployment: the chart refuses to render without it.
NAMED = ("--set", "server.clusterId=test")


def render_config(*values: str) -> str:
    rendered = render_chart(*values)
    match = re.search(r"cat > \"\$\{config_path\}\" <<EOF\n(?P<config>.*?)\n    EOF", rendered, re.S)
    if not match:
        raise AssertionError("could not find generated Ursula config in helm output")
    config = match.group("config")
    assert_server_accepts(config)
    return config


def assert_server_accepts(config: str) -> None:
    """The server's rules (`ursula-config` validation): a config with
    `[[raft.peers]]` must set `server.cluster_id`, and with
    `server.cluster_listen` every peer needs a `client_url`."""
    parsed = tomllib.loads(config)
    peers = parsed.get("raft", {}).get("peers", [])
    if peers:
        if not parsed.get("server", {}).get("cluster_id"):
            raise AssertionError("a config with [[raft.peers]] renders no server.cluster_id")
    if parsed.get("server", {}).get("cluster_listen"):
        for peer in peers:
            if not peer.get("client_url"):
                raise AssertionError(f"peer {peer['node_id']} renders no client_url with server.cluster_listen")


def documents(rendered: str, kind: str) -> dict[str, str]:
    """Map each rendered resource of ``kind`` to its manifest text by name."""
    found: dict[str, str] = {}
    for document in re.split(r"^---$", rendered, flags=re.M):
        if not re.search(rf"^kind: {kind}$", document, re.M):
            continue
        name = re.search(r"^metadata:\n(?:[ \t].*\n)*? {2}name: (?P<name>\S+)$", document, re.M)
        found[name.group("name") if name else ""] = document
    return found


def service_ports(document: str) -> list[tuple[str, int]]:
    return [
        (name, int(port))
        for name, port in re.findall(r"^ {4}- name: (\S+)\n {6}port: (\d+)$", document, re.M)
    ]


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
        "serviceAccount.name=ursula-storage",
        "--set",
        "serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::123456789012:role/server",
        "--set",
        "s3.prefix=server-data",
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
        rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt")

        self.assertIn(
            'if [ -r "${rollout_state_dir}/phase" ] && [ -r "${rollout_state_dir}/node-id" ]; then',
            rendered,
        )
        self.assertIn("        restarting)\n", rendered)
        self.assertNotIn("upgrading-restart-quiesce", rendered)
        self.assertIn('if [ "${rollout_node_id}" = "${node_id}" ]; then', rendered)
        self.assertIn(
            'export URSULA_START_MAINTENANCE_DRAINED="${start_maintenance_drained}"',
            rendered,
        )
        self.assertNotIn("start_maintenance_drained =", render_config(*NAMED, "--set", "s3.bucket=bkt"))
        self.assertIn(
            "- name: rollout-state\n              mountPath: /var/run/ursula-rollout-state\n              readOnly: true",
            rendered,
        )
        self.assertIn(
            "- name: rollout-state\n          configMap:\n            name: test-ursula-rollout-state\n            optional: true",
            rendered,
        )

    def test_matching_deployment_contract_renders(self) -> None:
        render_chart(*NAMED, *deployment_contract_values())

    def test_deployment_contract_rejects_namespace_drift(self) -> None:
        values = list(deployment_contract_values())
        values[1] = "another-namespace"

        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *NAMED, *values],
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
            ["helm", "template", "test", "charts/ursula", *NAMED, *values],
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
        rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt")

        self.assertIn(
            "podManagementPolicy: Parallel\n  updateStrategy:\n    type: RollingUpdate",
            rendered,
        )
        self.assertNotIn("app.kubernetes.io/component: ondelete-migration", rendered)

    def test_server_update_strategy_stages_on_delete_with_migration_hook(self) -> None:
        rendered = render_chart(*NAMED, 
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
        rendered = render_chart(*NAMED, 
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
        rendered = render_chart(*NAMED, 
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

    def test_rollout_rbac_has_no_schema_one_controller_revision_permission(self) -> None:
        for reserved in ("true", "false"):
            with self.subTest(maintenance_reservation=reserved):
                rendered = render_chart(*NAMED, 
                    "--set", "s3.bucket=bkt", "--set", "server.updateStrategy=OnDelete",
                    "--set", "server.gracefulRollout.enabled=true", "--set",
                    f"server.gracefulRollout.maintenanceReservation={reserved}",
                )
                self.assertNotIn("controllerrevisions", rendered)
                self.assertIn('resources: ["statefulsets"]', rendered)

    def test_shared_maintenance_job_rbac_and_inventory(self) -> None:
        values = (
            "--namespace", "test", "--set", "s3.bucket=bkt", "--set",
            "server.updateStrategy=OnDelete", "--set", "server.gracefulRollout.enabled=true",
            "--set", "server.gracefulRollout.maintenanceReservation=true",
            "--set", "server.coreCount=2", "--set", "raft.groupCount=256",
        )
        rendered = render_chart(*NAMED, *values)
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
        other = render_chart(*NAMED, *values, "--namespace", "other")
        other_role = re.search(r"kind: ClusterRole\nmetadata:\n  name: ([^\n]+)", other).group(1)
        this_role = re.search(r"kind: ClusterRole\nmetadata:\n  name: ([^\n]+)", rendered).group(1)
        self.assertNotEqual(this_role, other_role)
        for invalid in ("server.replicaCount=2", "server.gracefulRollout.expectedGroups=1"):
            with self.subTest(invalid=invalid):
                run = subprocess.run(["helm", "template", "test", "charts/ursula", *NAMED,
                                      *values, "--set", invalid], text=True, capture_output=True)
                self.assertNotEqual(run.returncode, 0)

    def test_startup_ownership_is_explicit_and_has_only_exact_store_mutation(self) -> None:
        values = ("--namespace", "test", "--set", "s3.bucket=bkt", "--set",
                  "server.updateStrategy=OnDelete", "--set", "server.gracefulRollout.enabled=true",
                  "--set", "server.gracefulRollout.maintenanceReservation=true",
                  "--set", "server.startupOwnership.enabled=true")
        rendered = render_chart(*NAMED, *values)
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
        self.assertNotIn('URSULA_STARTUP_RESERVATION', render_chart(*NAMED, "--set", "s3.bucket=bkt"))
        for invalid in ("server.replicaCount=2", "server.updateStrategy=RollingUpdate",
                        "server.gracefulRollout.maintenanceReservation=false",
                        "serviceAccount.create=false,serviceAccount.name=default",
                        "server.extraEnv[0].name=URSULA_STARTUP_RESERVATION,server.extraEnv[0].value=false"):
            with self.subTest(invalid=invalid):
                result = subprocess.run(["helm", "template", "test", "charts/ursula", *NAMED, *values, "--set", invalid], text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0)

    def test_every_deployment_role_uses_the_unified_ursula_binary(self) -> None:
        rendered = render_chart(*NAMED, 
            "--set",
            "gateway.enabled=true",
        )

        self.assertNotIn("/usr/local/bin/ursulagw", rendered)
        self.assertIn("- /usr/local/bin/ursula\n          args:\n            - gateway", rendered)
        self.assertIn(
            'exec /usr/local/bin/ursula server --config "${config_path}"',
            rendered,
        )

    def test_gateway_can_prefer_same_zone_service_endpoints(self) -> None:
        rendered = render_chart(*NAMED, 
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
        rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt")

        self.assertNotIn("trafficDistribution:", rendered)

    def test_gateway_passes_the_upstream_tcp_user_timeout(self) -> None:
        flag = '- --upstream-tcp-user-timeout\n            - "{}"'
        self.assertIn(flag.format(5), render_chart(*NAMED, "--set", "s3.bucket=bkt"))
        self.assertIn(
            flag.format(30),
            render_chart(*NAMED, "--set", "s3.bucket=bkt", "--set", "gateway.upstreamTcpUserTimeoutSeconds=30"),
        )
        # The gateway refuses 0, so the chart does too.
        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *NAMED, "--set", "s3.bucket=bkt",
             "--set", "gateway.upstreamTcpUserTimeoutSeconds=0"],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_gateway_body_budget_is_rendered_only_when_set_and_must_hold_the_limit(self) -> None:
        # Unset, the flag is not rendered: a gateway before 0.7.6 exits on it.
        self.assertNotIn("--body-budget-bytes", render_chart(*NAMED, "--set", "s3.bucket=bkt"))
        limit = (*NAMED, "--set", "s3.bucket=bkt", "--set", "gateway.maxRequestBodyBytes=33554432")
        self.assertIn(
            '- --body-budget-bytes\n            - "67108864"',
            render_chart(*limit, "--set", "gateway.bodyBudgetBytes=67108864"),
        )
        # Set, a budget that cannot hold a body at the limit beside the 32 MiB
        # headroom makes the gateway exit at startup, so the chart refuses it.
        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *limit, "--set", "gateway.bodyBudgetBytes=67108863"],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("gateway.bodyBudgetBytes", result.stderr)
        # A limit above the protocol's makes the gateway exit at startup.
        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *NAMED, "--set", "s3.bucket=bkt",
             "--set", "gateway.maxRequestBodyBytes=33554433"],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("maxRequestBodyBytes", result.stderr)

    def test_snapshot_prefix_follows_the_servers_rule(self) -> None:
        # The schema accepts exactly what the server's `SnapshotPrefix`
        # accepts: every leading and trailing '/' trimmed, then components
        # separated by single '/'s, none of them '.' or '..'.
        for prefix in ("snapshots", "/snapshots/", "a/b", "//a", "a//", ".hidden", "..x"):
            config = render_config(
                *NAMED, "--set", "s3.bucket=bkt", "--set", "snapshotStore.backend=s3",
                "--set-string", f"snapshotStore.prefix={prefix}",
            )
            self.assertIn(f"s3_prefix = {json.dumps(prefix)}", config)
        for prefix in ("", "/", ".", "..", "./a", "a/.", "a/../b", "a//b", " snapshots", "snap*"):
            result = subprocess.run(
                ["helm", "template", "test", "charts/ursula", *NAMED, "--set", "s3.bucket=bkt",
                 "--set", "snapshotStore.backend=s3", "--set-string", f"snapshotStore.prefix={prefix}"],
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0, prefix)
            self.assertIn("prefix", result.stderr, prefix)

    def test_admin_plane_defaults_to_loopback(self) -> None:
        config = render_config(*NAMED, "--set", "s3.bucket=bkt")

        self.assertEqual(tomllib.loads(config)["server"]["admin_listen"], "127.0.0.1:4438")

    def test_admin_plane_can_bind_for_trusted_in_cluster_operator(self) -> None:
        config = render_config(*NAMED, 
            "--set",
            "s3.bucket=bkt",
            "--set",
            "server.adminListen=0.0.0.0:4438",
        )

        self.assertEqual(tomllib.loads(config)["server"]["admin_listen"], "0.0.0.0:4438")

    def test_cluster_plane_listens_on_its_own_port_by_default(self) -> None:
        rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt")
        config = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt"))
        host = "test-ursula-{}.test-ursula-headless.default.svc.cluster.local"

        self.assertEqual(config["server"]["listen"], "0.0.0.0:4437")
        self.assertEqual(config["server"]["cluster_listen"], "0.0.0.0:4439")
        self.assertEqual(
            [(peer["url"], peer["client_url"]) for peer in config["raft"]["peers"]],
            [(f"http://{host.format(i)}:4439", f"http://{host.format(i)}:4437") for i in range(3)],
        )
        services = documents(rendered, "Service")
        self.assertEqual(service_ports(services["test-ursula"]), [("client", 4437)])
        self.assertEqual(
            service_ports(services["test-ursula-headless"]), [("client", 4437), ("cluster", 4439)]
        )
        self.assertIn(
            "- name: cluster\n              containerPort: 4439\n",
            documents(rendered, "StatefulSet")["test-ursula"],
        )
        # http_url is the Raft address, and ursulactl reads readiness on
        # the client plane.
        manifest = re.search(r"cluster-manifest\.json: \|\n(?P<json>(?: {4}.*\n)+)", rendered)
        nodes = json.loads(manifest.group("json"))["nodes"]
        self.assertEqual(
            [(node["http_url"], node["client_url"]) for node in nodes],
            [(f"http://{host.format(i)}:4439", f"http://{host.format(i)}:4437") for i in range(3)],
        )
        # The gateway is a client: its upstreams stay on the client port, so
        # a leader redirect names one of them.
        for i in range(3):
            self.assertIn(f"- \"http://{host.format(i)}:4437\"", rendered)

    def test_cluster_plane_can_share_the_client_port(self) -> None:
        values = (*NAMED, "--set", "s3.bucket=bkt", "--set", "server.clusterListen=false")
        rendered = render_chart(*values)
        config = tomllib.loads(render_config(*values))

        self.assertNotIn("cluster_listen", config["server"])
        for peer in config["raft"]["peers"]:
            self.assertTrue(peer["url"].endswith(":4437"), peer)
            self.assertNotIn("client_url", peer)
        services = documents(rendered, "Service")
        self.assertEqual(service_ports(services["test-ursula-headless"]), [("client", 4437)])
        self.assertNotIn("name: cluster", documents(rendered, "StatefulSet")["test-ursula"])

    def test_cluster_port_must_differ_from_the_client_and_admin_ports(self) -> None:
        for port, message in (
            ("4437", "must differ from server.ports.client"),
            ("4438", "must differ from the server.adminListen port"),
        ):
            with self.subTest(port=port):
                result = subprocess.run(
                    ["helm", "template", "test", "charts/ursula", *NAMED, "--set", "s3.bucket=bkt",
                     "--set", f"server.ports.cluster={port}"],
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
        # A shared port has no cluster port to collide.
        render_chart(*NAMED, "--set", "s3.bucket=bkt", "--set", "server.clusterListen=false",
                     "--set", "server.ports.cluster=4437")

    def test_graceful_rollout_names_the_cluster_port(self) -> None:
        for cluster_listen, port in (("true", "4439"), ("false", "4437")):
            with self.subTest(cluster_listen=cluster_listen):
                rendered = render_chart(
                    *NAMED, "--set", "s3.bucket=bkt", "--set", "server.updateStrategy=OnDelete",
                    "--set", "server.gracefulRollout.enabled=true",
                    "--set", f"server.clusterListen={cluster_listen}",
                )
                self.assertIn(f"- name: CLUSTER_PORT\n              value: \"{port}\"", rendered)

    def test_peers_manifest_and_rollout_agree_on_the_peer_host(self) -> None:
        # A long fullname makes the headless Service name truncate, and a
        # custom domain replaces cluster.local. The rollout manifest's
        # http_url names each node as its peers' [[raft.peers]] urls do.
        fullname = "u" * 58
        values = (
            *NAMED, "--set", "s3.bucket=bkt", "--set", "server.updateStrategy=OnDelete",
            "--set", "server.gracefulRollout.enabled=true", "--set", "global.clusterDomain=corp.example",
            "--set", f"fullnameOverride={fullname}",
        )
        rendered = render_chart(*values)
        peers = [peer["url"] for peer in tomllib.loads(render_config(*values))["raft"]["peers"]]
        manifest = re.search(r"cluster-manifest\.json: \|\n(?P<json>(?: {4}.*\n)+)", rendered)
        self.assertEqual([node["http_url"] for node in json.loads(manifest.group("json"))["nodes"]], peers)
        self.assertTrue(all(".svc.corp.example:" in url for url in peers))

        job = documents(rendered, "Job")[f"{fullname}-graceful-rollout"[:63].rstrip("-")]
        env = dict(re.findall(r"- name: ([A-Z_]+)\n {14}value: \"([^\"]*)\"", job))
        script = subprocess.run(
            ["sh", "-c", '. charts/ursula/files/graceful-rollout.sh && MANIFEST=$(mktemp) && write_manifest '
             '&& cat "${MANIFEST}" && rm -f "${MANIFEST}"'],
            env={
                "PATH": "/usr/bin:/bin", "NAMESPACE": "default", "ROLLOUT_SOURCE_ONLY": "1",
                **{key: env[key] for key in (
                    "STATEFULSET", "REPLICAS", "EXPECTED_GROUPS", "TARGET_IMAGE", "CLUSTER_PORT",
                    "CLUSTER_DOMAIN", "HEADLESS_SERVICE",
                )},
            },
            capture_output=True, text=True, check=True,
        )
        self.assertEqual([node["http_url"] for node in json.loads(script.stdout)["nodes"]], peers)

    def test_network_policy_is_off_by_default(self) -> None:
        self.assertNotIn("test-ursula", documents(render_chart(*NAMED, "--set", "s3.bucket=bkt"), "NetworkPolicy"))

    def test_network_policy_admits_every_client_and_only_peers_on_the_cluster_port(self) -> None:
        rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt", "--set", "server.networkPolicy.enabled=true")
        policy = documents(rendered, "NetworkPolicy")["test-ursula"]

        self.assertIn(
            "  podSelector:\n    matchLabels:\n      app.kubernetes.io/name: ursula\n"
            "      app.kubernetes.io/instance: test\n  policyTypes:\n    - Ingress\n",
            policy,
        )
        # The client rule names no source, so it admits every one.
        self.assertIn(
            "    - ports:\n        - protocol: TCP\n          port: 4437\n"
            "    # Cluster plane: only this release's server pods.\n",
            policy,
        )
        self.assertIn(
            "    - ports:\n        - protocol: TCP\n          port: 4439\n      from:\n"
            "        - podSelector:\n            matchLabels:\n"
            "              app.kubernetes.io/name: ursula\n              app.kubernetes.io/instance: test\n",
            policy,
        )
        self.assertNotIn("4438", policy)

    def test_network_policy_limits_client_ingress_to_the_configured_sources(self) -> None:
        rendered = render_chart(
            *NAMED, "--set", "s3.bucket=bkt", "--set", "server.networkPolicy.enabled=true",
            "--set", "server.networkPolicy.clientIngressFrom[0].namespaceSelector.matchLabels.team=streams",
        )
        policy = documents(rendered, "NetworkPolicy")["test-ursula"]

        self.assertIn(
            "          port: 4437\n      from:\n"
            "        - namespaceSelector:\n            matchLabels:\n              team: streams\n"
            "        - podSelector:\n            matchLabels:\n"
            "              app.kubernetes.io/name: ursula-gateway\n              app.kubernetes.io/instance: test\n"
            "        - podSelector:\n            matchLabels:\n"
            "              app.kubernetes.io/name: ursula-test\n              app.kubernetes.io/instance: test\n"
            "              app.kubernetes.io/component: helm-test\n",
            policy,
        )
        # The test pod's own labels match that selector, and not the
        # server Services' selector.
        test_pod = documents(rendered, "Pod")["test-ursula-wait-ready"]
        self.assertIn(
            "    app.kubernetes.io/name: ursula-test\n    app.kubernetes.io/instance: test\n"
            "    app.kubernetes.io/component: helm-test\n",
            test_pod,
        )

        bare = documents(render_chart(
            *NAMED, "--set", "s3.bucket=bkt", "--set", "server.networkPolicy.enabled=true",
            "--set", "server.networkPolicy.clientIngressFrom[0].podSelector.matchLabels.role=client",
            "--set", "gateway.enabled=false", "--set", "tests.enabled=false",
        ), "NetworkPolicy")["test-ursula"]
        self.assertNotIn("ursula-gateway", bare)
        self.assertNotIn("helm-test", bare)

    def test_network_policy_refuses_an_admin_plane_off_loopback(self) -> None:
        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *NAMED, "--set", "s3.bucket=bkt",
             "--set", "server.networkPolicy.enabled=true", "--set", "server.adminListen=0.0.0.0:4438"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("server.adminListen (0.0.0.0:4438) is not on loopback", result.stderr)
        # Off loopback without the policy, or on loopback with it, renders.
        render_chart(*NAMED, "--set", "s3.bucket=bkt", "--set", "server.adminListen=0.0.0.0:4438")
        render_chart(*NAMED, "--set", "s3.bucket=bkt", "--set", "server.networkPolicy.enabled=true",
                     "--set", "server.adminListen=localhost:4438")

    def test_client_port_must_differ_from_the_admin_port(self) -> None:
        result = subprocess.run(
            ["helm", "template", "test", "charts/ursula", *NAMED, "--set", "s3.bucket=bkt",
             "--set", "server.adminListen=127.0.0.1:4437"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("server.ports.client must differ from the server.adminListen port", result.stderr)

    def test_network_policy_with_a_shared_port_admits_peers_on_the_client_port(self) -> None:
        rendered = render_chart(
            *NAMED, "--set", "s3.bucket=bkt", "--set", "server.networkPolicy.enabled=true",
            "--set", "server.clusterListen=false",
        )
        policy = documents(rendered, "NetworkPolicy")["test-ursula"]

        self.assertNotIn("4439", policy)
        self.assertIn(
            "    - ports:\n        - protocol: TCP\n          port: 4437\n      from:\n"
            "        - podSelector:\n",
            policy,
        )

    def test_cluster_id_is_required_for_every_replica_count(self) -> None:
        # The chart renders [[raft.peers]] for every replica count, and a
        # node with peers refuses to start without server.cluster_id.
        for replicas in ("1", "3"):
            with self.subTest(replicas=replicas):
                unnamed = subprocess.run(
                    ["helm", "template", "rel", "charts/ursula", "--set", "s3.bucket=bkt",
                     "--set", f"server.replicaCount={replicas}"],
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(unnamed.returncode, 0)
                self.assertIn("server.clusterId is required", unnamed.stderr)
                config = tomllib.loads(render_config(
                    *NAMED, "--set", "s3.bucket=bkt", "--set", f"server.replicaCount={replicas}"))
                self.assertEqual(len(config["raft"]["peers"]), int(replicas))
                self.assertEqual(config["server"]["cluster_id"], "test")

    def test_cluster_id_can_be_set_and_is_validated(self) -> None:
        config = render_config("--set", "s3.bucket=bkt", "--set", "server.clusterId=prod-eu-1")
        self.assertEqual(tomllib.loads(config)["server"]["cluster_id"], "prod-eu-1")
        for invalid in ("a/b", "x" * 65):
            with self.assertRaises(subprocess.CalledProcessError):
                render_chart("--set", "s3.bucket=bkt", "--set", f"server.clusterId={invalid}")

    def test_max_uncommitted_value_uses_single_raft_table(self) -> None:
        config = render_config(*NAMED, "--set", "raft.maxUncommittedBytesPerGroup=8388608", "--set", "s3.bucket=bkt")

        raft_table_count = sum(line.strip() == "[raft]" for line in config.splitlines())
        self.assertEqual(raft_table_count, 1)
        parsed = tomllib.loads(config)
        self.assertEqual(parsed["raft"]["max_uncommitted_size_per_group"], "8388608")

    def test_max_uncommitted_zero_is_rendered(self) -> None:
        config = render_config(*NAMED, "--set", "raft.maxUncommittedBytesPerGroup=0", "--set", "s3.bucket=bkt")
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["raft"]["max_uncommitted_size_per_group"], "0")

    def test_wal_disk_watermarks_and_http_readiness_are_rendered(self) -> None:
        rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt")
        config = render_config(*NAMED, "--set", "s3.bucket=bkt")
        wal = tomllib.loads(config)["raft"]["wal"]

        self.assertEqual(wal["min_available_size"], "536870912")
        self.assertEqual(wal["resume_available_size"], "1073741824")
        self.assertEqual(wal["segment_size"], "67108864")
        self.assertEqual(wal["cache_size"], "268435456")
        self.assertIn(
            "readinessProbe:\n            httpGet:\n              path: /__ursula/ready\n              port: client",
            rendered,
        )

    def test_wal_segment_and_cache_sizes_are_rendered_and_validated(self) -> None:
        wal = tomllib.loads(
            render_config(*NAMED, 
                "--set",
                "s3.bucket=bkt",
                "--set",
                "raft.walSegmentBytes=8388608",
                "--set",
                "raft.walCacheBytes=33554432",
            )
        )["raft"]["wal"]
        self.assertEqual(wal["segment_size"], "8388608")
        self.assertEqual(wal["cache_size"], "33554432")

        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "raft.walSegmentBytes=1024",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("walSegmentBytes", result.stderr)

    def test_wal_fsync_policy_is_rendered_and_validated(self) -> None:
        wal = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt"))["raft"]["wal"]
        self.assertEqual(wal["fsync"], "never")

        wal = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt", "--set", "raft.walFsync=always"))[
            "raft"
        ]["wal"]
        self.assertEqual(wal["fsync"], "always")

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

    def test_every_server_pod_keeps_its_wal_on_the_raft_data_pvc(self) -> None:
        ownership = (
            "--namespace",
            "test",
            "--set",
            "server.updateStrategy=OnDelete",
            "--set",
            "server.gracefulRollout.enabled=true",
            "--set",
            "server.gracefulRollout.maintenanceReservation=true",
            "--set",
            "server.startupOwnership.enabled=true",
        )
        cases = {
            "one voter": ("--set", "server.replicaCount=1"),
            "three voters": ("--set", "server.replicaCount=3"),
            "five voters": ("--set", "server.replicaCount=5"),
            "shared maintenance and startup ownership": ownership,
        }
        for name, values in cases.items():
            with self.subTest(name=name):
                rendered = render_chart(*NAMED, "--set", "s3.bucket=bkt", *values)
                statefulset = re.search(
                    r"kind: StatefulSet\n.*?(?=\n---|\Z)", rendered, re.S
                ).group(0)
                self.assertIn(
                    "- name: raft-data\n              mountPath: /var/lib/ursula/raft",
                    statefulset,
                )
                self.assertIn(
                    "  volumeClaimTemplates:\n    - metadata:\n        name: raft-data\n",
                    statefulset,
                )
                self.assertIn('storage: "20Gi"', statefulset)
                self.assertNotIn("- name: raft-data\n          emptyDir", statefulset)
                wal = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt", *values))["raft"]["wal"]
                self.assertEqual(wal["path"], "/var/lib/ursula/raft")
                self.assertNotIn("backend", wal)
                self.assertNotIn("allow_volatile_multi_peer", wal)

    def test_removed_storage_values_are_refused(self) -> None:
        for removed in (
            "raft.storageMode=memory",
            "raft.allowVolatileMultiPeer=true",
            "persistence.enabled=false",
            "raft.logDir=",
        ):
            with self.subTest(removed=removed):
                result = subprocess.run(
                    [
                        "helm",
                        "template",
                        "test",
                        "charts/ursula",
                        "--set",
                        "s3.bucket=bkt",
                        "--set",
                        removed,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(removed.split("=", 1)[0].rsplit(".", 1)[1], result.stderr)

    def test_removed_indexer_values_are_refused(self) -> None:
        for removed in (
            "indexer.enabled=false",
            "deploymentContract.indexerRoleArn=arn:aws:iam::123456789012:role/indexer",
        ):
            with self.subTest(removed=removed):
                result = subprocess.run(
                    [
                        "helm",
                        "template",
                        "test",
                        "charts/ursula",
                        "--set",
                        "s3.bucket=bkt",
                        "--set",
                        removed,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("indexer", result.stderr)

    def test_cold_max_hot_bytes_zero_is_rendered(self) -> None:
        config = render_config(*NAMED, 
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
        config = render_config(*NAMED, 
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
        config = render_config(*NAMED, 
            "--set",
            "raft.snapshotPressureMaxGroupsPerTick=8",
            "--set",
            "raft.snapshotBuildMaxConcurrency=1",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["raft"]["snapshot_pressure_max_groups_per_tick"], 8)
        self.assertEqual(parsed["raft"]["snapshot_build_max_concurrency"], 1)

    def test_snapshot_build_concurrency_defaults_to_the_server_default(self) -> None:
        # Unset, the key is not rendered, so each server keeps its own
        # default: a pod that still runs an older image too.
        parsed = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt"))

        self.assertNotIn("snapshot_build_max_concurrency", parsed["raft"])

    def test_deleted_data_max_age_renders_only_when_set(self) -> None:
        # Unset, the key is not rendered, so a pod that still runs an older
        # image never gets a key it does not know.
        parsed = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt"))
        self.assertNotIn("deleted_data_max_age", parsed["raft"])

        config = render_config(
            *NAMED,
            "--set",
            "raft.deletedDataMaxAgeMs=7200000",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)
        self.assertEqual(parsed["raft"]["deleted_data_max_age"], "7200000ms")

    def test_deleted_data_max_age_below_one_is_refused(self) -> None:
        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "raft.deletedDataMaxAgeMs=0",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("deletedDataMaxAgeMs", result.stderr)

    def test_snapshot_build_concurrency_below_one_is_refused(self) -> None:
        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "raft.snapshotBuildMaxConcurrency=0",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("snapshotBuildMaxConcurrency", result.stderr)

    def test_snapshot_s3_renders_complete_config(self) -> None:
        config = render_config(*NAMED, "--set", "snapshotStore.backend=s3", "--set", "s3.bucket=bkt")
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "s3")
        self.assertEqual(parsed["storage"]["cold"]["s3"]["bucket"], "bkt")

    def test_snapshot_drive_interval_is_rendered(self) -> None:
        config = render_config(*NAMED, 
            "--set",
            "snapshotStore.driveIntervalMs=10000",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertEqual(parsed["storage"]["snapshot"]["drive_interval"], "10000ms")

    def test_snapshot_backstop_logs_renders_only_when_set(self) -> None:
        parsed = tomllib.loads(render_config(*NAMED, "--set", "s3.bucket=bkt"))
        self.assertNotIn("snapshot_backstop_logs", parsed["raft"])
        parsed = tomllib.loads(
            render_config(*NAMED, "--set", "s3.bucket=bkt", "--set", "raft.snapshotBackstopLogs=5000")
        )
        self.assertEqual(parsed["raft"]["snapshot_backstop_logs"], 5000)

    def test_snapshot_drive_interval_zero_is_refused(self) -> None:
        result = subprocess.run(
            [
                "helm",
                "template",
                "test",
                "charts/ursula",
                "--set",
                "s3.bucket=bkt",
                "--set",
                "snapshotStore.driveIntervalMs=0",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("driveIntervalMs", result.stderr)

    def test_cold_cache_zero_can_disable_default_cache(self) -> None:
        config = render_config(*NAMED, 
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
        config = render_config(*NAMED, 
            "--set",
            "coldStorage.enabled=true",
            "--set",
            "coldStorage.cache=null",
            "--set",
            "s3.bucket=bkt",
        )
        parsed = tomllib.loads(config)

        self.assertNotIn("cache", parsed["storage"]["cold"])

    def test_defaults_enable_compaction_and_follow_the_runtime_snapshot_default(self) -> None:
        parsed = tomllib.loads(render_config(*NAMED, "--set", "coldStorage.enabled=true", "--set", "s3.bucket=bkt"))

        self.assertIs(parsed["storage"]["cold"]["compaction_enabled"], True)
        # "auto" is Ursula's own default: S3 snapshots with an S3 cold store,
        # inline otherwise. The chart no longer pins "inline".
        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "auto")
        self.assertEqual(parsed["storage"]["snapshot"]["s3_prefix"], "snapshots")

    def test_default_snapshot_backend_without_cold_storage_needs_no_bucket(self) -> None:
        parsed = tomllib.loads(render_config(*NAMED, ))

        self.assertEqual(parsed["storage"]["cold"]["backend"], "none")
        self.assertEqual(parsed["storage"]["snapshot"]["backend"], "auto")
        self.assertNotIn("s3", parsed["storage"]["cold"])

    def test_explicit_inline_snapshots_and_disabled_compaction_still_render(self) -> None:
        parsed = tomllib.loads(
            render_config(*NAMED, 
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
            render_config(*NAMED, 
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
