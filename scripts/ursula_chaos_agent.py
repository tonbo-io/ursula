#!/usr/bin/env python3
"""Long-running Ursula chaos workload and status publisher.

Run this on the client EC2 instance. It continuously appends deterministic
payloads to one Ursula stream, verifies readable offsets, samples node metrics,
randomly stops one EC2 node at a time, starts it again, and publishes a compact
status JSON for the docs `/status` page.
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BUCKET = "chaos"
CONTENT_TYPE = "application/octet-stream"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def run(argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@dataclass(frozen=True)
class Node:
    name: str
    instance_id: str
    base_url: str


class ChaosAgent:
    def __init__(self, args: argparse.Namespace) -> None:
        self.nodes = [parse_node(raw) for raw in args.node]
        if not self.nodes:
            raise SystemExit("at least one --node is required")
        self.status_file = args.status_file
        self.status_s3_uri = args.status_s3_uri
        self.append_per_second = args.append_per_second
        self.payload_bytes = args.payload_bytes
        self.verify_every = args.verify_every
        self.status_every = args.status_every
        self.fault_min_secs = args.fault_min_secs
        self.fault_max_secs = args.fault_max_secs
        self.recovery_secs = args.recovery_secs
        self.disable_faults = args.disable_faults
        self.timeout_secs = args.timeout_secs
        self.started_at = utc_now()
        self.stream = args.stream or f"run-{self.started_at.strftime('%Y%m%d%H%M%S')}"
        self.next_offset = 0
        self.append_success = 0
        self.append_errors = 0
        self.verified_offsets = 0
        self.mismatch_count = 0
        self.last_integrity_error: str | None = None
        self.last_integrity_check: datetime | None = None
        self.recent_payloads: deque[tuple[int, bytes]] = deque(maxlen=1024)
        self.events: deque[dict[str, Any]] = deque(maxlen=32)
        self.active_fault: tuple[Node, datetime] | None = None
        self.last_fault: str | None = None
        self.next_fault_at = self.choose_next_fault()

    def choose_next_fault(self) -> datetime | None:
        if self.disable_faults:
            return None
        return utc_now() + timedelta(seconds=random.randint(self.fault_min_secs, self.fault_max_secs))

    def event(self, level: str, message: str) -> None:
        self.events.appendleft({"time": iso(utc_now()), "level": level, "message": message})
        print(f"{iso(utc_now())} {level.upper()} {message}", flush=True)

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        request = urllib.request.Request(url, data=body, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_secs) as response:
                return response.status, response.read(), dict(response.headers.items())
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read(), dict(exc.headers.items())

    def create_stream(self) -> None:
        for node in self.nodes:
            status, _, _ = self.request("PUT", f"{node.base_url}/{BUCKET}/{self.stream}")
            if status in {200, 201, 409}:
                self.event("info", f"stream {self.stream} ready via {node.name} status={status}")
                return
        raise RuntimeError("unable to create chaos stream on any node")

    def append_once(self) -> None:
        start_offset = self.next_offset
        payload = f"{self.append_success:020d}:{start_offset:020d}\n".encode()
        if len(payload) < self.payload_bytes:
            payload += b"x" * (self.payload_bytes - len(payload))
        first_node = self.append_success % len(self.nodes)
        last_error = "no target nodes"
        for attempt in range(len(self.nodes)):
            node = self.nodes[(first_node + attempt) % len(self.nodes)]
            try:
                status, _, headers = self.request(
                    "POST",
                    f"{node.base_url}/{BUCKET}/{self.stream}",
                    body=payload,
                    headers={
                        "Content-Type": CONTENT_TYPE,
                        "Producer-Id": "chaos-agent",
                        "Producer-Epoch": "1",
                        "Producer-Seq": str(self.append_success),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                last_error = f"{node.name}: {exc}"
                continue
            if status not in {200, 204}:
                last_error = f"{node.name}: status={status}"
                continue
            next_offset_header = headers.get("Stream-Next-Offset")
            if next_offset_header is not None:
                try:
                    self.next_offset = max(self.next_offset + len(payload), int(next_offset_header))
                except ValueError:
                    self.next_offset += len(payload)
            else:
                self.next_offset += len(payload)
            self.recent_payloads.append((start_offset, payload))
            self.append_success += 1
            return
        self.append_errors += 1
        self.event("warn", f"append failed on all nodes: {last_error}")

    def verify_integrity(self) -> None:
        if not self.recent_payloads:
            return
        start_offset, expected = self.recent_payloads[-1]
        last_error: str | None = None
        for node in self.nodes:
            status, body, _ = self.request(
                "GET",
                f"{node.base_url}/{BUCKET}/{self.stream}?{urllib.parse.urlencode({'offset': start_offset, 'max-bytes': len(expected)})}",
            )
            if status == 200 and body.startswith(expected):
                self.verified_offsets += 1
                self.last_integrity_error = None
                self.last_integrity_check = utc_now()
                return
            if status in {204, 404, 410, 503}:
                last_error = f"{node.name} read status={status}"
                continue
            last_error = f"{node.name} read status={status} body_prefix={body[:32]!r}"
        self.mismatch_count += 1
        self.last_integrity_error = last_error or "readback mismatch"
        self.last_integrity_check = utc_now()
        self.event("error", f"integrity check failed: {self.last_integrity_error}")

    def sample_node(self, node: Node) -> dict[str, Any]:
        sample: dict[str, Any] = {
            "name": node.name,
            "role": "node",
            "instance_id": node.instance_id,
        }
        try:
            state = json.loads(
                run(
                    [
                        "aws",
                        "ec2",
                        "describe-instances",
                        "--instance-ids",
                        node.instance_id,
                        "--query",
                        "Reservations[0].Instances[0].State.Name",
                        "--output",
                        "json",
                    ]
                ).stdout
            )
            sample["instance_state"] = state
        except Exception as exc:  # noqa: BLE001
            sample["instance_state"] = "unknown"
            sample["last_error"] = f"describe-instance: {exc}"
        try:
            status, body, _ = self.request("GET", f"{node.base_url}/__ursula/metrics")
            sample["metrics_state"] = "ok" if status == 200 else f"http_{status}"
            if status == 200:
                metrics = json.loads(body)
                raft_groups = metrics.get("raft_groups", [])
                sample["accepted_appends"] = metrics.get("accepted_appends")
                sample["applied_mutations"] = metrics.get("applied_mutations")
                sample["raft_groups"] = len(raft_groups)
                sample["leader_groups"] = sum(1 for group in raft_groups if group.get("current_leader") is not None)
        except Exception as exc:  # noqa: BLE001
            sample["metrics_state"] = "unavailable"
            sample["last_error"] = str(exc)
        return sample

    def maybe_inject_fault(self) -> None:
        now = utc_now()
        if self.disable_faults:
            return
        if self.active_fault is not None:
            node, recover_at = self.active_fault
            if now < recover_at:
                return
            self.event("warn", f"starting {node.name} after chaos stop")
            run(["aws", "ec2", "start-instances", "--instance-ids", node.instance_id], check=False)
            self.last_fault = f"stopped and restarted {node.name}"
            self.active_fault = None
            self.next_fault_at = self.choose_next_fault()
            return
        if self.next_fault_at is None or now < self.next_fault_at:
            return
        node = random.choice(self.nodes)
        self.event("warn", f"stopping {node.name} for chaos fault")
        run(["aws", "ec2", "stop-instances", "--instance-ids", node.instance_id], check=False)
        self.active_fault = (node, now + timedelta(seconds=self.recovery_secs))

    def build_status(self) -> dict[str, Any]:
        nodes = [self.sample_node(node) for node in self.nodes]
        running_nodes = sum(1 for node in nodes if node.get("instance_state") == "running")
        metrics_ok = sum(1 for node in nodes if node.get("metrics_state") == "ok")
        integrity_status = "operational" if self.mismatch_count == 0 and self.last_integrity_error is None else "major_outage"
        if running_nodes >= 2 and metrics_ok >= 2 and integrity_status == "operational":
            overall = "operational" if self.active_fault is None else "degraded_performance"
        elif running_nodes >= 2:
            overall = "partial_outage"
        else:
            overall = "major_outage"
        active_fault = None
        if self.active_fault is not None:
            node, recover_at = self.active_fault
            active_fault = f"{node.name} stopped until {iso(recover_at)}"
        return {
            "schema_version": 1,
            "overall": overall,
            "started_at": iso(self.started_at),
            "updated_at": iso(utc_now()),
            "summary": f"{running_nodes}/3 nodes running, {metrics_ok}/3 metrics endpoints healthy",
            "workload": {
                "append_target_per_second": self.append_per_second,
                "append_success_total": self.append_success,
                "append_error_total": self.append_errors,
                "last_append_offset": self.next_offset,
                "stream": self.stream,
            },
            "integrity": {
                "status": integrity_status,
                "checked_at": iso(self.last_integrity_check),
                "verified_offsets": self.verified_offsets,
                "mismatch_count": self.mismatch_count,
                "last_error": self.last_integrity_error,
            },
            "chaos": {
                "enabled": not self.disable_faults,
                "active_fault": active_fault,
                "last_fault": self.last_fault,
                "next_fault_after": iso(self.next_fault_at),
            },
            "nodes": nodes,
            "events": list(self.events),
        }

    def publish_status(self) -> None:
        status = self.build_status()
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.status_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
        tmp.replace(self.status_file)
        if self.status_s3_uri:
            run(
                [
                    "aws",
                    "s3",
                    "cp",
                    str(self.status_file),
                    self.status_s3_uri,
                    "--content-type",
                    "application/json",
                    "--cache-control",
                    "no-store",
                ],
                check=False,
            )

    def run_forever(self) -> None:
        self.create_stream()
        self.event("info", "chaos agent started")
        last_status = 0.0
        interval = 1.0 / max(1, self.append_per_second)
        while True:
            loop_started = time.monotonic()
            self.append_once()
            if self.append_success % self.verify_every == 0:
                self.verify_integrity()
            self.maybe_inject_fault()
            if loop_started - last_status >= self.status_every:
                self.publish_status()
                last_status = loop_started
            elapsed = time.monotonic() - loop_started
            if elapsed < interval:
                time.sleep(interval - elapsed)


def parse_node(raw: str) -> Node:
    parts = raw.split("=", 2)
    if len(parts) != 3:
        raise SystemExit("--node must be name=instance-id=http://host:port")
    name, instance_id, base_url = parts
    return Node(name=name, instance_id=instance_id, base_url=base_url.rstrip("/"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Ursula 24/7 EC2 chaos agent")
    parser.add_argument("--node", action="append", default=[], help="name=instance-id=http://host:port")
    parser.add_argument("--status-file", type=Path, default=Path("/tmp/ursula-chaos/status.json"))
    parser.add_argument("--status-s3-uri", default="")
    parser.add_argument("--stream", default="")
    parser.add_argument("--append-per-second", type=int, default=20)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--verify-every", type=int, default=50)
    parser.add_argument("--status-every", type=int, default=15)
    parser.add_argument("--fault-min-secs", type=int, default=900)
    parser.add_argument("--fault-max-secs", type=int, default=1800)
    parser.add_argument("--recovery-secs", type=int, default=180)
    parser.add_argument("--timeout-secs", type=int, default=3)
    parser.add_argument("--disable-faults", action="store_true")
    return parser


def main() -> int:
    agent = ChaosAgent(build_parser().parse_args())
    try:
        agent.run_forever()
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"fatal: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
