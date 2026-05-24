#!/usr/bin/env python3
"""Small root-only node fault daemon for the Ursula EC2 chaos test."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


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
        self.peer_hosts: list[str] = []
        self.tc = command_path("tc")
        self.iptables = command_path("iptables")

    def clear(self) -> None:
        run([self.tc, "qdisc", "del", "dev", self.dev, "root"])
        for host in self.peer_hosts:
            run([self.iptables, "-D", "INPUT", "-s", host, "-j", "DROP"])
            run([self.iptables, "-D", "OUTPUT", "-d", host, "-j", "DROP"])
        self.peer_hosts = []

    def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.clear()
        kind = payload.get("kind")
        if kind == "netem":
            delay_ms = max(0, int(payload.get("delay_ms", 0)))
            jitter_ms = max(0, int(payload.get("jitter_ms", 0)))
            loss_percent = max(0.0, min(100.0, float(payload.get("loss_percent", 0))))
            args = [self.tc, "qdisc", "replace", "dev", self.dev, "root", "netem"]
            if delay_ms > 0:
                args.extend(["delay", f"{delay_ms}ms"])
                if jitter_ms > 0:
                    args.append(f"{jitter_ms}ms")
            if loss_percent > 0:
                args.extend(["loss", f"{loss_percent}%"])
            result = run(args)
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "tc failed")
            return {"ok": True, "kind": kind, "dev": self.dev}
        if kind == "partition":
            hosts = [str(host) for host in payload.get("peer_hosts", []) if host]
            for host in hosts:
                run([self.iptables, "-A", "INPUT", "-s", host, "-j", "DROP"], check=True)
                run([self.iptables, "-A", "OUTPUT", "-d", host, "-j", "DROP"], check=True)
            self.peer_hosts = hosts
            return {"ok": True, "kind": kind, "peer_hosts": hosts}
        raise ValueError(f"unsupported fault kind: {kind}")


class Handler(BaseHTTPRequestHandler):
    state: FaultState

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/clear":
                self.state.clear()
                self.write_json(200, {"ok": True})
                return
            if self.path == "/apply":
                length = int(self.headers.get("content-length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                self.write_json(200, self.state.apply(payload))
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
