#!/usr/bin/env python3
"""Small EC2 ops helper for static Ursula clusters.

This intentionally stays close to the deployment shape used by the migration
benchmarks: existing EC2 instances, EC2 Instance Connect for short-lived SSH
keys, static Raft peers, and one optional benchmark client.
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
        binary=raw.get("binary", "/tmp/ursula-http"),
        pid_prefix=raw.get("pid_prefix", "/tmp/ursula"),
        log_prefix=raw.get("log_prefix", "/tmp/ursula"),
        core_count=int(raw.get("core_count", 16)),
        raft_group_count=int(raw.get("raft_group_count", 64)),
        raft_memory=bool(raw.get("raft_memory", True)),
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
            f"if [ -f {pid} ]; then "
            f"kill $(cat {pid}) 2>/dev/null || true; "
            f"rm -f {pid}; "
            "fi"
        )
        self.ssh(node, command, check=False)

    def start_node(self, node: Node) -> None:
        self.stop_node(node)
        env_lines = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in sorted(self.config.cold_env.items()))
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
        if self.config.init_membership_per_group:
            args.append("--raft-init-membership-per-group")
        command = "\n".join(
            [
                "set -euo pipefail",
                f"test -x {shlex.quote(self.config.binary)}",
                env_lines,
                " ".join(shlex.quote(arg) for arg in args)
                + f" > {shlex.quote(self.remote_log(node))} 2>&1 & echo $! > {shlex.quote(self.remote_pid(node))}",
            ]
        )
        self.ssh(node, command)

    def metrics(self, node: Node) -> dict[str, Any]:
        command = f"curl -fsS http://127.0.0.1:{self.config.port}/__ursula/metrics"
        result = self.ssh(node, command, capture=True)
        return json.loads(result.stdout)

    def status(self) -> None:
        for node in self.config.nodes:
            pid = shlex.quote(self.remote_pid(node))
            command = f"ps -p $(cat {pid} 2>/dev/null || echo 0) -o pid,stat,etime,pcpu,pmem,command 2>/dev/null || true"
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
                for item in snapshots:
                    leaders.extend(
                        group["current_leader"]
                        for group in item.get("raft_groups", [])
                        if group.get("current_leader") is not None
                    )
                leader_set = set(leaders)
                expected = {node.id for node in self.config.nodes}
                last = (group_counts, {leader: leaders.count(leader) for leader in sorted(leader_set)})
                if all(count == self.config.raft_group_count for count in group_counts) and expected.issubset(leader_set):
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operate a static Ursula EC2 cluster")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--known-hosts", default=DEFAULT_KNOWN_HOSTS)
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("start")
    sub.add_parser("stop")
    sub.add_parser("status")
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    ops = Ec2Ops(load_config(args.config), args.known_hosts, args.verbose)
    try:
        if args.command == "start":
            for node in ops.config.nodes:
                ops.start_node(node)
        elif args.command == "stop":
            for node in ops.config.nodes:
                ops.stop_node(node)
        elif args.command == "status":
            ops.status()
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
        else:
            raise AssertionError(args.command)
    finally:
        ops.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
