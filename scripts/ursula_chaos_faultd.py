#!/usr/bin/env python3
"""Small root-only node fault daemon for the Ursula EC2 chaos test.

Supports scoped faults so cluster-plane impairments don't bleed into
S3 traffic (and vice versa). Each `apply` call replaces all previously
applied state.

Fault payload schema:
  kind            One of "netem" | "partition" | "s3_unavailable" | "clear"
  scope           Used by "netem": "cluster" | "all" (default "all")
  delay_ms/jitter_ms/loss_percent  netem parameters
  peer_hosts      partition: list of remote IPs to drop both directions
  cluster_subnets netem scope=cluster: list of CIDRs to selectively delay
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


S3_SNI_PATTERNS = [
    "s3.amazonaws.com",
    "s3.us-east-1.amazonaws.com",
]


def unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def run(argv: list[str], *, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def command_path(name: str) -> str:
    found = shutil.which(name)
    if found:
        return found
    for candidate in (f"/usr/sbin/{name}", f"/sbin/{name}", f"/usr/bin/{name}", f"/bin/{name}"):
        if shutil.which(candidate) or subprocess.run(["test", "-x", candidate]).returncode == 0:
            return candidate
    return name


def default_dev() -> str:
    route = run(["ip", "route", "show", "default"])
    if route.returncode == 0:
        parts = route.stdout.split()
        if "dev" in parts:
            idx = parts.index("dev")
            if idx + 1 < len(parts):
                return parts[idx + 1]
    links = run(["ip", "-o", "link", "show"])
    for line in links.stdout.splitlines():
        name = line.split(":", 2)[1].strip()
        if name != "lo":
            return name
    return "eth0"


class FaultState:
    def __init__(self, dev: str) -> None:
        self.dev = default_dev() if dev == "auto" else dev
        self.tc = command_path("tc")
        self.iptables = command_path("iptables")
        self.peer_hosts: list[str] = []
        self.s3_patterns: list[str] = []
        self.has_root_qdisc = False
        self.has_classful_qdisc = False
        # Stale iptables/tc rules from a prior chaos run survive faultd restart
        # because they live in the kernel, not in this process. If the new run
        # never targets this node with /apply (so clear() never fires), the
        # rules silently impair the node forever — exactly the s3_unavailable
        # iptables DROP that wedged N3's cold flush in the 2026-05-31 incident.
        # Clear at startup so a fresh process always starts from a clean kernel.
        self.clear()

    def clear(
        self,
        *,
        peer_hosts: list[str] | None = None,
        s3_patterns: list[str] | None = None,
    ) -> None:
        # Stateless: always attempt to remove the root qdisc. A daemon restart
        # or a concurrent ThreadingHTTPServer request can lose has_*_qdisc,
        # which would otherwise leave a netem delay/loss qdisc permanently
        # impairing this node's cluster plane — a "fault" that never clears.
        run([self.tc, "qdisc", "del", "dev", self.dev, "root"])
        self.has_root_qdisc = False
        self.has_classful_qdisc = False
        for host in unique([*self.peer_hosts, *(peer_hosts or [])]):
            while run([self.iptables, "-D", "INPUT", "-s", host, "-j", "DROP"]).returncode == 0:
                pass
            while run([self.iptables, "-D", "OUTPUT", "-d", host, "-j", "DROP"]).returncode == 0:
                pass
        self.peer_hosts = []
        # Stateless cleanup: remove rules for every known SNI pattern, not just
        # what this process tracked. A daemon restart (or a concurrent
        # ThreadingHTTPServer request) can lose self.s3_patterns, which would
        # otherwise leave an OUTPUT DROP rule in place and permanently block the
        # node's S3 — wedging cold flush long after the fault was "cleared".
        # Loop each -D until it fails so duplicate rules are fully removed.
        for pattern in unique([*self.s3_patterns, *S3_SNI_PATTERNS, *(s3_patterns or [])]):
            while run([self.iptables, "-D", "OUTPUT", "-p", "tcp", "--dport", "443",
                       "-m", "string", "--algo", "bm", "--string", pattern,
                       "-j", "DROP"]).returncode == 0:
                pass
        self.s3_patterns = []

    def status(
        self,
        *,
        peer_hosts: list[str] | None = None,
        s3_patterns: list[str] | None = None,
    ) -> dict[str, Any]:
        qdisc = run([self.tc, "qdisc", "show", "dev", self.dev])
        qdisc_lines = qdisc.stdout.splitlines() if qdisc.returncode == 0 else []
        active_qdisc = [
            line
            for line in qdisc_lines
            if line.startswith("qdisc netem ") or line.startswith("qdisc prio ")
        ]

        partition_rules: list[dict[str, str]] = []
        for host in unique([*self.peer_hosts, *(peer_hosts or [])]):
            if run([self.iptables, "-C", "INPUT", "-s", host, "-j", "DROP"]).returncode == 0:
                partition_rules.append({"direction": "input", "host": host})
            if run([self.iptables, "-C", "OUTPUT", "-d", host, "-j", "DROP"]).returncode == 0:
                partition_rules.append({"direction": "output", "host": host})

        s3_rules: list[str] = []
        for pattern in unique([*self.s3_patterns, *S3_SNI_PATTERNS, *(s3_patterns or [])]):
            if run([self.iptables, "-C", "OUTPUT", "-p", "tcp", "--dport", "443",
                    "-m", "string", "--algo", "bm", "--string", pattern,
                    "-j", "DROP"]).returncode == 0:
                s3_rules.append(pattern)

        qdisc_error = ""
        if qdisc.returncode != 0:
            qdisc_error = qdisc.stderr.strip() or qdisc.stdout.strip()
        clean = not active_qdisc and not partition_rules and not s3_rules and not qdisc_error
        return {
            "clean": clean,
            "dev": self.dev,
            "active_qdisc": active_qdisc,
            "partition_rules": partition_rules,
            "s3_rules": s3_rules,
            "qdisc_error": qdisc_error,
        }

    def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.clear()
        kind = payload.get("kind")
        if kind == "clear":
            status = self.status()
            return {"ok": status["clean"], "kind": "clear", **status}
        if kind == "netem":
            return self._apply_netem(payload)
        if kind == "partition":
            return self._apply_partition(payload)
        if kind == "s3_unavailable":
            return self._apply_s3_unavailable(payload)
        raise ValueError(f"unsupported fault kind: {kind}")

    def _apply_netem(self, payload: dict[str, Any]) -> dict[str, Any]:
        delay_ms = max(0, int(payload.get("delay_ms", 0)))
        jitter_ms = max(0, int(payload.get("jitter_ms", 0)))
        loss_percent = max(0.0, min(100.0, float(payload.get("loss_percent", 0))))
        scope = payload.get("scope", "all")

        netem_args = ["netem"]
        if delay_ms > 0:
            netem_args.extend(["delay", f"{delay_ms}ms"])
            if jitter_ms > 0:
                netem_args.append(f"{jitter_ms}ms")
                # Bound reordering so chaos models real network latency rather
                # than producing TCP HOL stalls from packet reorder.
                netem_args.extend(["25%"])
        if loss_percent > 0:
            netem_args.extend(["loss", f"{loss_percent}%"])

        if scope == "cluster":
            cluster_subnets = [str(c) for c in payload.get("cluster_subnets", []) if c]
            if not cluster_subnets:
                raise ValueError("netem scope=cluster requires cluster_subnets")
            # prio qdisc: band 0 = default fast path, band 1 = netem-impaired.
            # Filters classify dst-matching packets into band 1.
            self._must_run([self.tc, "qdisc", "add", "dev", self.dev,
                            "root", "handle", "1:", "prio"])
            self.has_classful_qdisc = True
            self._must_run([self.tc, "qdisc", "add", "dev", self.dev,
                            "parent", "1:1", "handle", "10:"] + netem_args)
            for cidr in cluster_subnets:
                self._must_run([self.tc, "filter", "add", "dev", self.dev,
                                "parent", "1:0", "protocol", "ip",
                                "prio", "1", "u32",
                                "match", "ip", "dst", cidr,
                                "flowid", "1:1"])
            return {"ok": True, "kind": "netem", "scope": "cluster",
                    "dev": self.dev, "cluster_subnets": cluster_subnets}

        # scope == "all" — preserves previous root-qdisc behavior
        self._must_run([self.tc, "qdisc", "replace", "dev", self.dev, "root"] + netem_args)
        self.has_root_qdisc = True
        return {"ok": True, "kind": "netem", "scope": "all", "dev": self.dev}

    def _apply_partition(self, payload: dict[str, Any]) -> dict[str, Any]:
        hosts = [str(host) for host in payload.get("peer_hosts", []) if host]
        for host in hosts:
            run([self.iptables, "-A", "INPUT", "-s", host, "-j", "DROP"], check=True)
            run([self.iptables, "-A", "OUTPUT", "-d", host, "-j", "DROP"], check=True)
        self.peer_hosts = hosts
        return {"ok": True, "kind": "partition", "peer_hosts": hosts}

    def _apply_s3_unavailable(self, payload: dict[str, Any]) -> dict[str, Any]:
        patterns = [str(p) for p in payload.get("patterns", S3_SNI_PATTERNS) if p]
        for pattern in patterns:
            run([self.iptables, "-A", "OUTPUT", "-p", "tcp", "--dport", "443",
                 "-m", "string", "--algo", "bm", "--string", pattern, "-j", "DROP"],
                check=True)
        self.s3_patterns = patterns
        return {"ok": True, "kind": "s3_unavailable", "patterns": patterns}

    def _must_run(self, argv: list[str]) -> None:
        result = run(argv)
        if result.returncode != 0:
            raise RuntimeError(
                result.stderr.strip() or result.stdout.strip()
                or f"command failed: {' '.join(argv)}"
            )


class Handler(BaseHTTPRequestHandler):
    state: FaultState

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/clear":
                payload = self.read_json_body()
                peer_hosts = [str(host) for host in payload.get("peer_hosts", []) if host]
                s3_patterns = [str(pattern) for pattern in payload.get("s3_patterns", []) if pattern]
                self.state.clear(peer_hosts=peer_hosts, s3_patterns=s3_patterns)
                status = self.state.status(peer_hosts=peer_hosts, s3_patterns=s3_patterns)
                self.write_json(200 if status["clean"] else 500, {"ok": status["clean"], **status})
                return
            if self.path == "/apply":
                payload = self.read_json_body()
                self.write_json(200, self.state.apply(payload))
                return
            self.write_json(404, {"ok": False, "error": "not found"})
        except Exception as exc:  # noqa: BLE001
            self.write_json(500, {"ok": False, "error": str(exc)})

    def do_GET(self) -> None:  # noqa: N802
        try:
            if self.path == "/status":
                status = self.state.status()
                self.write_json(200 if status["clean"] else 500, {"ok": status["clean"], **status})
                return
            self.write_json(404, {"ok": False, "error": "not found"})
        except Exception as exc:  # noqa: BLE001
            self.write_json(500, {"ok": False, "error": str(exc)})

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("content-length", "0"))
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length) or b"{}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Ursula chaos node fault daemon")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=4492)
    parser.add_argument("--dev", default="auto")
    args = parser.parse_args()
    Handler.state = FaultState(args.dev)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
