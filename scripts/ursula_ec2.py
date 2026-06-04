#!/usr/bin/env python3
"""Small EC2 ops helper for static Ursula clusters.

This intentionally stays close to the deployment shape used by the migration
benchmarks: existing EC2 instances, EC2 Instance Connect for short-lived SSH
keys, static Raft peers, and one optional benchmark client.

Operational verbs are progressively migrating to `crates/ursula-ctl`
(`ursulactl`), which talks to the raft-aware HTTP surface directly and runs
the same drain / wait-ready safety checks under madsim DST. As of today the
Rust CLI covers `restart`, `status`, and `wait-ready`; the SSH-dependent
verbs (`upload-binary`, `install-binary`, `install-chaos-agent`, `deploy-
chaos`, ...) still live here because they require host-level filesystem
access. AWS deployment scaffolding (IAM / EC2 lifecycle / security groups)
stays in this script permanently.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_KNOWN_HOSTS = "/tmp/ursula-ec2-known-hosts"


@dataclass(frozen=True)
class Node:
    id: int
    name: str
    instance_id: str
    az: str
    public_ip: str
    private_ip: str


@dataclass(frozen=True)
class ClientHost:
    name: str
    instance_id: str
    az: str
    public_ip: str
    private_ip: str | None


@dataclass(frozen=True)
class ClusterConfig:
    nodes: list[Node]
    client: ClientHost | None
    ssh_user: str
    port: int
    binary: str
    pid_prefix: str
    log_prefix: str
    core_count: int
    raft_group_count: int
    raft_memory: bool
    raft_log_prefix: str | None
    init_membership_per_group: bool
    cold_env: dict[str, str]
    perf_compare: str | None


def run(argv: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def load_config(path: Path) -> ClusterConfig:
    try:
        raw = json.loads(path.read_text())
    except OSError as exc:
        raise SystemExit(f"read config {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"parse config {path}: {exc}") from exc
    nodes = [
        Node(
            id=int(item["id"]),
            name=item.get("name", f"node-{item['id']}"),
            instance_id=item["instance_id"],
            az=item["az"],
            public_ip=item["public_ip"],
            private_ip=item["private_ip"],
        )
        for item in raw["nodes"]
    ]
    nodes.sort(key=lambda node: node.id)
    client_raw = raw.get("client")
    client = None
    if client_raw:
        client = ClientHost(
            name=client_raw.get("name", "client"),
            instance_id=client_raw["instance_id"],
            az=client_raw["az"],
            public_ip=client_raw["public_ip"],
            private_ip=client_raw.get("private_ip"),
        )
    return ClusterConfig(
        nodes=nodes,
        client=client,
        ssh_user=raw.get("ssh_user", "ec2-user"),
        port=int(raw.get("port", 4491)),
        binary=raw.get("binary", "/tmp/ursula"),
        pid_prefix=raw.get("pid_prefix", "/tmp/ursula"),
        log_prefix=raw.get("log_prefix", "/tmp/ursula"),
        core_count=int(raw.get("core_count", 16)),
        raft_group_count=int(raw.get("raft_group_count", 64)),
        raft_memory=bool(raw.get("raft_memory", True)),
        raft_log_prefix=raw.get("raft_log_prefix"),
        init_membership_per_group=bool(raw.get("init_membership_per_group", True)),
        cold_env={str(k): str(v) for k, v in raw.get("cold_env", {}).items()},
        perf_compare=raw.get("perf_compare"),
    )


class Ec2Ops:
    def __init__(self, config: ClusterConfig, known_hosts: str, verbose: bool) -> None:
        self.config = config
        self.known_hosts = known_hosts
        self.verbose = verbose
        self._key_dir = tempfile.TemporaryDirectory(prefix="ursula-ec2-key-")
        self.key_path = Path(self._key_dir.name) / "id_ed25519"
        run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(self.key_path)])

    def close(self) -> None:
        self._key_dir.cleanup()

    def send_key(self, instance_id: str, az: str) -> None:
        run(
            [
                "aws",
                "ec2-instance-connect",
                "send-ssh-public-key",
                "--instance-id",
                instance_id,
                "--availability-zone",
                az,
                "--instance-os-user",
                self.config.ssh_user,
                "--ssh-public-key",
                f"file://{self.key_path}.pub",
            ]
        )

    def ssh_args(self, public_ip: str) -> list[str]:
        return [
            "ssh",
            "-i",
            str(self.key_path),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"UserKnownHostsFile={self.known_hosts}",
            "-o",
            "ConnectTimeout=10",
            f"{self.config.ssh_user}@{public_ip}",
        ]

    def ssh(self, node: Node | ClientHost, command: str, *, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
        self.send_key(node.instance_id, node.az)
        if self.verbose:
            print(f"+ ssh {node.name}: {command}", file=sys.stderr)
        return run(self.ssh_args(node.public_ip) + [command], check=check, capture=capture)

    def scp_to(self, node: Node | ClientHost, local: Path, remote: str) -> None:
        self.send_key(node.instance_id, node.az)
        if self.verbose:
            print(f"+ scp {local} {node.name}:{remote}", file=sys.stderr)
        run(
            [
                "scp",
                "-i",
                str(self.key_path),
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                f"UserKnownHostsFile={self.known_hosts}",
                "-o",
                "ConnectTimeout=10",
                str(local),
                f"{self.config.ssh_user}@{node.public_ip}:{remote}",
            ]
        )

    def chmod_executable(self, node: Node | ClientHost, remote: str) -> None:
        self.ssh(node, f"chmod +x {shlex.quote(remote)}")

    def install_file(self, node: Node | ClientHost, local: Path, remote: str) -> None:
        temp_remote = f"/tmp/{local.name}.{os.getpid()}.upload"
        self.scp_to(node, local, temp_remote)
        self.ssh(
            node,
            "set -euo pipefail\n"
            f"sudo mkdir -p {shlex.quote(str(Path(remote).parent))}\n"
            f"sudo install -m 0755 {shlex.quote(temp_remote)} {shlex.quote(remote)}\n"
            f"rm -f {shlex.quote(temp_remote)}",
        )

    def all_peer_flags(self) -> list[str]:
        flags: list[str] = []
        for node in self.config.nodes:
            flags += ["--raft-peer", f"{node.id}=http://{node.private_ip}:{self.config.port}"]
        return flags

    def remote_pid(self, node: Node) -> str:
        return f"{self.config.pid_prefix}-{node.id}.pid"

    def remote_log(self, node: Node) -> str:
        return f"{self.config.log_prefix}-{node.id}.log"

    def stop_node(self, node: Node) -> None:
        pid = shlex.quote(self.remote_pid(node))
        command = (
            "if systemctl list-unit-files ursula-chaos.service >/dev/null 2>&1; then "
            "sudo systemctl stop ursula-chaos.service || true; "
            f"elif [ -f {pid} ]; then "
            f"kill $(cat {pid}) 2>/dev/null || true; "
            f"rm -f {pid}; "
            "fi"
        )
        self.ssh(node, command, check=False)

    def start_node(self, node: Node) -> None:
        env_lines = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in sorted(self.config.cold_env.items()))
        command = "\n".join(
            [
                "set -euo pipefail",
                f"test -x {shlex.quote(self.config.binary)}",
                "if systemctl list-unit-files ursula-chaos.service >/dev/null 2>&1; then",
                "  sudo systemctl restart ursula-chaos.service",
                "  exit 0",
                "fi",
                f"if [ -f {shlex.quote(self.remote_pid(node))} ]; then "
                f"kill $(cat {shlex.quote(self.remote_pid(node))}) 2>/dev/null || true; "
                f"rm -f {shlex.quote(self.remote_pid(node))}; "
                "fi",
                env_lines,
                " ".join(shlex.quote(arg) for arg in self.node_command(node))
                + f" > {shlex.quote(self.remote_log(node))} 2>&1 & echo $! > {shlex.quote(self.remote_pid(node))}",
            ]
        )
        self.ssh(node, command)

    def node_command(self, node: Node) -> list[str]:
        args = [
            self.config.binary,
            "--listen",
            f"0.0.0.0:{self.config.port}",
            "--core-count",
            str(self.config.core_count),
            "--raft-group-count",
            str(self.config.raft_group_count),
            "--raft-node-id",
            str(node.id),
            *self.all_peer_flags(),
        ]
        if self.config.raft_memory:
            args.append("--raft-memory")
        elif self.config.raft_log_prefix:
            args += ["--raft-log-dir", f"{self.config.raft_log_prefix}-{node.id}"]
        if self.config.init_membership_per_group:
            args.append("--raft-init-membership-per-group")
        return args

    def install_service(self, node: Node, restart: bool = True) -> None:
        service_env = dict(self.config.cold_env)
        if self.config.raft_memory:
            service_env.setdefault(
                "URSULA_RAFT_MEMORY_BOOTSTRAP_MARKER_DIR",
                "/tmp/ursula-raft-memory-bootstrap",
            )
        env_lines = "\n".join(
            f"Environment={key}={value}"
            for key, value in sorted(service_env.items())
        )
        exec_start = " ".join(shlex.quote(arg) for arg in self.node_command(node))
        restart_policy = "no" if self.config.raft_memory else "always"
        restart_sec = "" if self.config.raft_memory else "RestartSec=3"
        unit = f"""[Unit]
Description=Ursula chaos node {node.id}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={self.config.ssh_user}
WorkingDirectory=/tmp
{env_lines}
ExecStart={exec_start}
Restart={restart_policy}
{restart_sec}
LimitNOFILE=1048576
LimitCORE=0

[Install]
WantedBy=multi-user.target
"""
        command = "\n".join(
            [
                "set -euo pipefail",
                f"test -x {shlex.quote(self.config.binary)}",
                *self.raft_log_dir_setup_commands(node),
                "sudo tee /etc/systemd/system/ursula-chaos.service >/dev/null <<'EOF'",
                unit,
                "EOF",
                "sudo systemctl daemon-reload",
                "sudo systemctl enable ursula-chaos.service",
            ]
        )
        if restart:
            command = "\n".join([command, "sudo systemctl restart ursula-chaos.service"])
        self.ssh(node, command)

    def raft_log_dir_setup_commands(self, node: Node) -> list[str]:
        if self.config.raft_memory or not self.config.raft_log_prefix:
            return []
        raft_log_dir = f"{self.config.raft_log_prefix}-{node.id}"
        return [
            f"sudo mkdir -p {shlex.quote(raft_log_dir)}",
            f"sudo chown -R {shlex.quote(self.config.ssh_user)}:{shlex.quote(self.config.ssh_user)} {shlex.quote(raft_log_dir)}",
        ]

    def metrics(self, node: Node) -> dict[str, Any]:
        command = f"curl -fsS http://127.0.0.1:{self.config.port}/__ursula/metrics"
        result = self.ssh(node, command, capture=True)
        return json.loads(result.stdout)

    def status(self) -> None:
        for node in self.config.nodes:
            pid = shlex.quote(self.remote_pid(node))
            command = (
                "if systemctl list-unit-files ursula-chaos.service >/dev/null 2>&1; then "
                "systemctl is-active ursula-chaos.service || true; "
                f"else ps -p $(cat {pid} 2>/dev/null || echo 0) -o pid,stat,etime,pcpu,pmem,command 2>/dev/null || true; "
                "fi"
            )
            result = self.ssh(node, command, capture=True, check=False)
            print(f"== {node.name} ({node.public_ip}) ==")
            print(result.stdout.rstrip() or "not running")
            try:
                metrics = self.metrics(node)
            except Exception as exc:  # noqa: BLE001
                print(f"metrics unavailable: {exc}")
                continue
            leaders = [group.get("current_leader") for group in metrics.get("raft_groups", []) if group.get("current_leader") is not None]
            counts = {leader: leaders.count(leader) for leader in sorted(set(leaders))}
            print(
                "metrics "
                f"groups={len(metrics.get('raft_groups', []))} "
                f"leaders={counts} "
                f"accepted_appends={metrics.get('accepted_appends')} "
                f"cold_hot_bytes={metrics.get('cold_hot_bytes')} "
                f"cold_upload_bytes={metrics.get('cold_flush_upload_bytes')}"
            )

    def wait_ready(self, timeout_secs: int) -> None:
        deadline = time.time() + timeout_secs
        last: Any = None
        while time.time() < deadline:
            try:
                snapshots = [self.metrics(node) for node in self.config.nodes]
                group_counts = [len(item.get("raft_groups", [])) for item in snapshots]
                leaders: list[int] = []
                leader_counts: list[int] = []
                for item in snapshots:
                    snapshot_leaders = [
                        group["current_leader"]
                        for group in item.get("raft_groups", [])
                        if group.get("current_leader") is not None
                    ]
                    leaders.extend(snapshot_leaders)
                    leader_counts.append(len(snapshot_leaders))
                leader_set = set(leaders)
                last = (group_counts, {leader: leaders.count(leader) for leader in sorted(leader_set)})
                if all(count == self.config.raft_group_count for count in group_counts) and all(
                    count == self.config.raft_group_count for count in leader_counts
                ):
                    print(f"ready groups={group_counts} leaders={last[1]}")
                    return
            except Exception as exc:  # noqa: BLE001
                last = repr(exc)
            time.sleep(1)
        raise SystemExit(f"cluster not ready after {timeout_secs}s: {last}")

    def cleanup_s3(self, root: str) -> None:
        bucket = self.config.cold_env.get("URSULA_COLD_S3_BUCKET")
        if not bucket:
            raise SystemExit("config cold_env must set URSULA_COLD_S3_BUCKET for cleanup-s3")
        run(["aws", "s3", "rm", "--recursive", f"s3://{bucket}/{root.rstrip('/')}/"])

    def run_perf(self, args: argparse.Namespace) -> None:
        if self.config.client is None:
            raise SystemExit("config does not define a client host")
        if self.config.perf_compare is None:
            raise SystemExit("config does not define perf_compare")
        command = self.perf_command(args.bucket, args.perf_arg)
        self.ssh(self.config.client, command)

    def perf_command(self, bucket: str, perf_args: list[str], *, target_node_index: int = 0) -> str:
        if self.config.perf_compare is None:
            raise SystemExit("config does not define perf_compare")
        target_node = self.config.nodes[target_node_index % len(self.config.nodes)]
        target = f"http://{target_node.private_ip}:{self.config.port}"
        read_bases = ",".join(f"http://{node.private_ip}:{self.config.port}" for node in self.config.nodes)
        perf_args = list(perf_args)
        if perf_args and perf_args[0] == "--":
            perf_args = perf_args[1:]
        return " ".join(
            shlex.quote(part)
            for part in [
                self.config.perf_compare,
                "--targets",
                "ursula",
                "--ursula",
                target,
                "--ursula-read-bases",
                read_bases,
                "--ursula-bucket",
                bucket,
                "--durable",
                "http://127.0.0.1:1",
                "--s2",
                "http://127.0.0.1:1",
                *perf_args,
            ]
        )

    def run_perf_many(self, args: argparse.Namespace) -> None:
        if self.config.client is None:
            raise SystemExit("config does not define a client host")
        if args.processes <= 0:
            raise SystemExit("--processes must be greater than zero")
        remote_dir = args.remote_dir.rstrip("/")
        commands = []
        for index in range(args.processes):
            bucket = f"{args.bucket_prefix}-{index:02d}"
            output = f"{remote_dir}/perf-{index:02d}.json"
            target_node_index = index if args.target_mode == "rotate" else 0
            target_node = self.config.nodes[target_node_index % len(self.config.nodes)]
            command = self.perf_command(bucket, args.perf_arg, target_node_index=target_node_index)
            target = f"http://{target_node.private_ip}:{self.config.port}"
            commands.append((index, bucket, target, output, command))
        script_lines = [
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(remote_dir)}",
            "rm -f " + shlex.quote(f"{remote_dir}/perf-") + "*.json " + shlex.quote(f"{remote_dir}/perf-") + "*.status",
            "pids=()",
        ]
        for index, bucket, target, output, command in commands:
            status = f"{remote_dir}/perf-{index:02d}.status"
            script_lines.append(
                f"echo start index={index} target={shlex.quote(target)} bucket={shlex.quote(bucket)} output={shlex.quote(output)}"
            )
            script_lines.append(
                f"({command} > {shlex.quote(output)} 2>&1; echo $? > {shlex.quote(status)}) &"
            )
            script_lines.append("pids+=(\"$!\")")
        script_lines += [
            "rc=0",
            "for pid in \"${pids[@]}\"; do wait \"$pid\" || rc=1; done",
            f"for status in {shlex.quote(remote_dir)}/perf-*.status; do "
            "[ -f \"$status\" ] || continue; "
            "code=$(cat \"$status\"); "
            "echo \"$status rc=$code\"; "
            "[ \"$code\" = 0 ] || rc=1; "
            "done",
            f"ls -lh {shlex.quote(remote_dir)}",
            "exit \"$rc\"",
        ]
        self.ssh(self.config.client, "\n".join(script_lines))

    def upload_binary(self, args: argparse.Namespace) -> None:
        local = args.local.expanduser().resolve()
        if not local.is_file():
            raise SystemExit(f"local binary does not exist: {local}")
        remote = args.remote
        targets: list[Node | ClientHost] = []
        if args.target in {"servers", "all"}:
            targets.extend(self.config.nodes)
        if args.target in {"client", "all"}:
            if self.config.client is None:
                raise SystemExit("config does not define a client host")
            targets.append(self.config.client)
        for target in targets:
            print(f"upload {local} -> {target.name}:{remote}")
            self.scp_to(target, local, remote)
            self.chmod_executable(target, remote)

    def install_binary(self, args: argparse.Namespace) -> None:
        local = args.local.expanduser().resolve()
        if not local.is_file():
            raise SystemExit(f"local binary does not exist: {local}")
        targets: list[Node | ClientHost] = []
        if args.target in {"servers", "all"}:
            targets.extend(self.config.nodes)
        if args.target in {"client", "all"}:
            if self.config.client is None:
                raise SystemExit("config does not define a client host")
            targets.append(self.config.client)
        for target in targets:
            print(f"install {local} -> {target.name}:{args.remote}")
            self.install_file(target, local, args.remote)

    def deploy_chaos(self, args: argparse.Namespace) -> None:
        if self.config.client is None:
            raise SystemExit("config does not define a client host")
        self.require_cold_store_for_chaos(args.allow_hot_only)
        binary = args.binary.expanduser().resolve()
        agent = args.agent.expanduser().resolve()
        faultd = args.faultd.expanduser().resolve()
        if not binary.is_file():
            raise SystemExit(f"binary does not exist: {binary}")
        if not agent.is_file():
            raise SystemExit(f"agent does not exist: {agent}")
        if not faultd.is_file():
            raise SystemExit(f"faultd does not exist: {faultd}")
        for node in self.config.nodes:
            print(f"install {binary} -> {node.name}:{self.config.binary}")
            self.install_file(node, binary, self.config.binary)
            print(f"install {faultd} -> {node.name}:{args.faultd_path}")
            self.install_file(node, faultd, args.faultd_path)
        print(f"install {agent} -> {self.config.client.name}:{args.agent_path}")
        self.install_file(self.config.client, agent, args.agent_path)
        self.install_services(restart=not args.no_restart_services)
        self.install_faultd(args)
        self.install_chaos_agent(args)

    def require_cold_store_for_chaos(self, allow_hot_only: bool) -> None:
        if allow_hot_only:
            return
        backend = self.config.cold_env.get("URSULA_COLD_BACKEND", "none").strip().lower()
        if backend in {"", "none", "disabled", "off"}:
            raise SystemExit(
                "chaos deployment requires cold_env. Set URSULA_COLD_BACKEND "
                "(s3 for multi-node chaos) and cold flush/admission limits, or pass "
                "--allow-hot-only for a deliberately hot-only smoke run."
            )
        if backend != "s3":
            raise SystemExit(
                "multi-node chaos deployment requires URSULA_COLD_BACKEND=s3. "
                "Raft replicates cold manifests, so the referenced objects must be readable "
                "from every node."
            )
        if not self.config.cold_env.get("URSULA_COLD_S3_BUCKET", "").strip():
            raise SystemExit("cold_env must set URSULA_COLD_S3_BUCKET when URSULA_COLD_BACKEND=s3")

    def install_services(self, restart: bool = True) -> None:
        for node in self.config.nodes:
            self.install_service(node, restart=restart)

    def install_chaos_agent(self, args: argparse.Namespace) -> None:
        if self.config.client is None:
            raise SystemExit("config does not define a client host")
        self.require_cold_store_for_chaos(args.allow_hot_only)
        node_args: list[str] = []
        for node in self.config.nodes:
            node_args.extend(
                [
                    "--node",
                    f"{node.name}={node.instance_id}=http://{node.private_ip}:{self.config.port}",
                ]
            )
        command_parts = [
            "/usr/bin/python3",
            args.agent_path,
            *node_args,
            "--status-s3-uri",
            args.status_s3_uri,
            "--stream-count",
            str(args.stream_count),
            "--append-per-second",
            str(args.append_per_second),
            "--payload-sizes",
            args.payload_sizes,
            "--payload-kinds",
            args.payload_kinds,
            "--producer-count",
            str(args.producer_count),
            "--epoch-bump-every",
            str(args.epoch_bump_every),
            "--producer-probe-every",
            str(args.producer_probe_every),
            "--reader-count",
            str(args.reader_count),
            "--verify-modes",
            args.verify_modes,
            "--verify-every",
            str(args.verify_every),
            "--old-sample-every",
            str(args.old_sample_every),
            "--status-every",
            str(args.status_every),
            "--fault-min-secs",
            str(args.fault_min_secs),
            "--fault-max-secs",
            str(args.fault_max_secs),
            "--fault-profile",
            args.fault_profile,
            "--recovery-secs",
            str(args.recovery_secs),
            "--recovery-slo-secs",
            str(args.recovery_slo_secs),
            "--repair-retry-secs",
            str(args.repair_retry_secs),
            "--max-repair-attempts",
            str(args.max_repair_attempts),
        ]
        if args.fault_scenarios:
            command_parts.extend(["--fault-scenarios", args.fault_scenarios])
        if args.first_fault_secs is not None:
            command_parts.extend(["--first-fault-secs", str(args.first_fault_secs)])
        exec_start = " ".join(shlex.quote(part) for part in command_parts)
        unit = f"""[Unit]
Description=Ursula chaos agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={self.config.ssh_user}
WorkingDirectory=/tmp
ExecStart={exec_start}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""
        command = "\n".join(
            [
                "set -euo pipefail",
                f"test -x {shlex.quote(args.agent_path)}",
                "command -v aws >/dev/null",
                "sudo tee /etc/systemd/system/ursula-chaos-agent.service >/dev/null <<'EOF'",
                unit,
                "EOF",
                "sudo systemctl daemon-reload",
                "sudo systemctl enable ursula-chaos-agent.service",
                "sudo systemctl restart ursula-chaos-agent.service",
            ]
        )
        self.ssh(self.config.client, command)

    def install_faultd(self, args: argparse.Namespace) -> None:
        unit = f"""[Unit]
Description=Ursula chaos node fault daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/python3 {shlex.quote(args.faultd_path)} --port {int(args.faultd_port)} --dev {shlex.quote(args.faultd_dev)}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""
        command = "\n".join(
            [
                "set -euo pipefail",
                f"test -x {shlex.quote(args.faultd_path)}",
                "if ! command -v tc >/dev/null && ! test -x /usr/sbin/tc && ! test -x /sbin/tc; then "
                "sudo dnf install -y iproute-tc iptables || sudo yum install -y iproute-tc iptables || true; "
                "fi",
                "command -v tc >/dev/null || test -x /usr/sbin/tc || test -x /sbin/tc",
                "sudo tee /etc/systemd/system/ursula-chaos-faultd.service >/dev/null <<'EOF'",
                unit,
                "EOF",
                "sudo systemctl daemon-reload",
                "sudo systemctl enable ursula-chaos-faultd.service",
                "sudo systemctl restart ursula-chaos-faultd.service",
            ]
        )
        for node in self.config.nodes:
            self.ssh(node, command)

    def chaos_agent_status(self) -> None:
        if self.config.client is None:
            raise SystemExit("config does not define a client host")
        result = self.ssh(
            self.config.client,
            "sudo systemctl status ursula-chaos-agent.service --no-pager -l || true; "
            "sudo journalctl -u ursula-chaos-agent.service -n 80 --no-pager || true",
            capture=True,
            check=False,
        )
        output = result.stdout.rstrip() or result.stderr.rstrip()
        print(output or "no chaos agent status")

    def service_status(self) -> None:
        for node in self.config.nodes:
            print(f"== {node.name} ({node.public_ip}) ==")
            result = self.ssh(
                node,
                "sudo systemctl status ursula-chaos.service --no-pager -l || true",
                capture=True,
                check=False,
            )
            output = result.stdout.rstrip() or result.stderr.rstrip()
            print(output or "no service status")

    def logs(self, args: argparse.Namespace) -> None:
        for node in self.config.nodes:
            print(f"== {node.name} ({node.public_ip}) ==")
            command = (
                f"tail -n {int(args.lines)} {shlex.quote(self.remote_log(node))} 2>/dev/null "
                f"|| sudo journalctl -u ursula-chaos.service -n {int(args.lines)} --no-pager 2>/dev/null "
                "|| true"
            )
            result = self.ssh(node, command, capture=True, check=False)
            print(result.stdout.rstrip() or "no log")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operate a static Ursula EC2 cluster")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--known-hosts", default=DEFAULT_KNOWN_HOSTS)
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("start")
    install_service = sub.add_parser("install-service")
    install_service.add_argument(
        "--no-restart",
        action="store_true",
        help="Write and enable the systemd unit without restarting the currently running node process.",
    )
    sub.add_parser("stop")
    sub.add_parser("status")
    logs = sub.add_parser("logs")
    logs.add_argument("--lines", type=int, default=80)
    sub.add_parser("service-status")
    ready = sub.add_parser("wait-ready")
    ready.add_argument("--timeout-secs", type=int, default=120)
    cleanup = sub.add_parser("cleanup-s3")
    cleanup.add_argument("--root", required=True)
    perf = sub.add_parser("perf")
    perf.add_argument("--bucket", required=True)
    perf.add_argument("perf_arg", nargs=argparse.REMAINDER)
    perf_many = sub.add_parser("perf-many")
    perf_many.add_argument("--processes", type=int, required=True)
    perf_many.add_argument("--bucket-prefix", required=True)
    perf_many.add_argument("--remote-dir", default="/tmp/ursula-perf-many")
    perf_many.add_argument(
        "--target-mode",
        choices=["rotate", "first"],
        default="rotate",
        help="Choose the Ursula endpoint for each client process. 'rotate' spreads processes across configured nodes.",
    )
    perf_many.add_argument("perf_arg", nargs=argparse.REMAINDER)
    upload = sub.add_parser("upload-binary")
    upload.add_argument("--local", required=True, type=Path)
    upload.add_argument("--remote", required=True)
    upload.add_argument("--target", choices=["servers", "client", "all"], default="servers")
    install = sub.add_parser("install-binary")
    install.add_argument("--local", required=True, type=Path)
    install.add_argument("--remote", required=True)
    install.add_argument("--target", choices=["servers", "client", "all"], default="servers")
    chaos_agent = sub.add_parser("install-chaos-agent")
    chaos_agent.add_argument("--agent-path", default="/opt/ursula/ursula_chaos_agent.py")
    chaos_agent.add_argument("--status-s3-uri", default="s3://ursula-chaos-status-tonbo/status.json")
    chaos_agent.add_argument("--stream-count", type=int, default=24)
    chaos_agent.add_argument("--append-per-second", type=int, default=20)
    chaos_agent.add_argument("--payload-sizes", default="128,1024,16384,65536")
    chaos_agent.add_argument("--payload-kinds", default="ascii,binary,zero,utf8")
    chaos_agent.add_argument("--producer-count", type=int, default=8)
    chaos_agent.add_argument("--epoch-bump-every", type=int, default=5000)
    chaos_agent.add_argument("--producer-probe-every", type=int, default=200)
    chaos_agent.add_argument("--reader-count", type=int, default=2)
    chaos_agent.add_argument("--verify-modes", default="latest,recent,old,cold")
    chaos_agent.add_argument("--verify-every", type=int, default=50)
    chaos_agent.add_argument("--old-sample-every", type=int, default=128)
    chaos_agent.add_argument("--burst-every", type=int, default=300)
    chaos_agent.add_argument("--burst-appends", type=int, default=200)
    chaos_agent.add_argument("--status-every", type=int, default=15)
    chaos_agent.add_argument("--fault-min-secs", type=int, default=900)
    chaos_agent.add_argument("--fault-max-secs", type=int, default=1800)
    chaos_agent.add_argument(
        "--fault-profile",
        choices=["network", "revert-detection", "custom"],
        default="network",
    )
    chaos_agent.add_argument(
        "--fault-scenarios",
        default=None,
    )
    chaos_agent.add_argument(
        "--allow-hot-only",
        action="store_true",
        help="Allow chaos runs without cold_env; this is only a hot-only smoke run and can OOM under sustained load.",
    )
    chaos_agent.add_argument("--first-fault-secs", type=int)
    chaos_agent.add_argument("--recovery-secs", type=int, default=180)
    chaos_agent.add_argument("--recovery-slo-secs", type=int, default=120)
    chaos_agent.add_argument("--repair-retry-secs", type=int, default=180)
    chaos_agent.add_argument("--max-repair-attempts", type=int, default=2)
    faultd = sub.add_parser("install-faultd")
    faultd.add_argument("--faultd-path", default="/opt/ursula/ursula_chaos_faultd.py")
    faultd.add_argument("--faultd-port", type=int, default=4492)
    faultd.add_argument("--faultd-dev", default="auto")
    deploy_chaos = sub.add_parser("deploy-chaos")
    deploy_chaos.add_argument("--binary", required=True, type=Path)
    deploy_chaos.add_argument("--agent", default=Path("scripts/ursula_chaos_agent.py"), type=Path)
    deploy_chaos.add_argument("--agent-path", default="/opt/ursula/ursula_chaos_agent.py")
    deploy_chaos.add_argument("--faultd", default=Path("scripts/ursula_chaos_faultd.py"), type=Path)
    deploy_chaos.add_argument("--faultd-path", default="/opt/ursula/ursula_chaos_faultd.py")
    deploy_chaos.add_argument("--faultd-port", type=int, default=4492)
    deploy_chaos.add_argument("--faultd-dev", default="auto")
    deploy_chaos.add_argument(
        "--no-restart-services",
        action="store_true",
        help="Install the future node service unit without restarting running nodes.",
    )
    deploy_chaos.add_argument("--status-s3-uri", default="s3://ursula-chaos-status-tonbo/status.json")
    deploy_chaos.add_argument("--stream-count", type=int, default=24)
    deploy_chaos.add_argument("--append-per-second", type=int, default=20)
    deploy_chaos.add_argument("--payload-sizes", default="128,1024,16384,65536")
    deploy_chaos.add_argument("--payload-kinds", default="ascii,binary,zero,utf8")
    deploy_chaos.add_argument("--producer-count", type=int, default=8)
    deploy_chaos.add_argument("--epoch-bump-every", type=int, default=5000)
    deploy_chaos.add_argument("--producer-probe-every", type=int, default=200)
    deploy_chaos.add_argument("--reader-count", type=int, default=2)
    deploy_chaos.add_argument("--verify-modes", default="latest,recent,old,cold")
    deploy_chaos.add_argument("--verify-every", type=int, default=50)
    deploy_chaos.add_argument("--old-sample-every", type=int, default=128)
    deploy_chaos.add_argument("--burst-every", type=int, default=0)
    deploy_chaos.add_argument("--burst-appends", type=int, default=0)
    deploy_chaos.add_argument("--status-every", type=int, default=15)
    deploy_chaos.add_argument("--fault-min-secs", type=int, default=900)
    deploy_chaos.add_argument("--fault-max-secs", type=int, default=1800)
    deploy_chaos.add_argument(
        "--fault-profile",
        choices=["network", "revert-detection", "custom"],
        default="network",
    )
    deploy_chaos.add_argument(
        "--fault-scenarios",
        default=None,
    )
    deploy_chaos.add_argument(
        "--allow-hot-only",
        action="store_true",
        help="Allow chaos runs without cold_env; this is only a hot-only smoke run and can OOM under sustained load.",
    )
    deploy_chaos.add_argument("--first-fault-secs", type=int)
    deploy_chaos.add_argument("--recovery-secs", type=int, default=180)
    deploy_chaos.add_argument("--recovery-slo-secs", type=int, default=120)
    deploy_chaos.add_argument("--repair-retry-secs", type=int, default=180)
    deploy_chaos.add_argument("--max-repair-attempts", type=int, default=2)
    sub.add_parser("chaos-agent-status")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    ops = Ec2Ops(load_config(args.config), args.known_hosts, args.verbose)
    try:
        if args.command == "start":
            for node in ops.config.nodes:
                ops.start_node(node)
        elif args.command == "install-service":
            ops.install_services(restart=not args.no_restart)
        elif args.command == "stop":
            for node in ops.config.nodes:
                ops.stop_node(node)
        elif args.command == "status":
            ops.status()
        elif args.command == "logs":
            ops.logs(args)
        elif args.command == "service-status":
            ops.service_status()
        elif args.command == "wait-ready":
            ops.wait_ready(args.timeout_secs)
        elif args.command == "cleanup-s3":
            ops.cleanup_s3(args.root)
        elif args.command == "perf":
            ops.run_perf(args)
        elif args.command == "perf-many":
            ops.run_perf_many(args)
        elif args.command == "upload-binary":
            ops.upload_binary(args)
        elif args.command == "install-binary":
            ops.install_binary(args)
        elif args.command == "install-chaos-agent":
            ops.install_chaos_agent(args)
        elif args.command == "install-faultd":
            ops.install_faultd(args)
        elif args.command == "deploy-chaos":
            ops.deploy_chaos(args)
        elif args.command == "chaos-agent-status":
            ops.chaos_agent_status()
        else:
            raise AssertionError(args.command)
    finally:
        ops.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
