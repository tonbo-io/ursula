#!/usr/bin/env python3
"""Long-running Ursula chaos workload and status publisher.

Run this on the client EC2 instance. It continuously appends deterministic
payloads to one Ursula stream, verifies readable offsets, samples node metrics,
randomly stops one EC2 node at a time, starts it again, and publishes a compact
status JSON for the docs `/status` page.
"""

from __future__ import annotations

import argparse
import hashlib
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
READ_AVAILABILITY_STATUSES = {204, 404, 410, 502, 503}
REVERT_DETECTION_SCENARIOS = {"no_allow_stop"}
# The public --raft-memory chaos run should keep a surviving quorum. Dropping
# two nodes in a three-node cluster tests data-loss behavior rather than
# recovery, especially when the leader is among the stopped nodes.
UNSUPPORTED_QUORUM_LOSS_SCENARIOS = {"two_node_stop", "quorum_loss"}
FAULT_PROFILES = {
    "network": "netem_delay,netem_loss,asymmetric_partition",
    "revert-detection": "no_allow_stop",
}
SETSUM_PRIMES = [
    4294967291,
    4294967279,
    4294967231,
    4294967197,
    4294967189,
    4294967161,
    4294967143,
    4294967111,
]


class Setsum:
    def __init__(self) -> None:
        self.state = [0] * len(SETSUM_PRIMES)

    def insert_vectored(self, pieces: list[bytes]) -> None:
        digest = hashlib.sha3_256(b"".join(pieces)).digest()
        for idx, prime in enumerate(SETSUM_PRIMES):
            value = int.from_bytes(digest[idx * 4 : idx * 4 + 4], "little")
            if value >= prime:
                value -= prime
            self.state[idx] = (self.state[idx] + value) % prime

    def hexdigest(self) -> str:
        return b"".join(value.to_bytes(4, "little") for value in self.state).hex()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_int_list(value: str) -> list[int]:
    sizes: list[int] = []
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        sizes.append(max(1, int(raw)))
    return sizes or [128]


def run(argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@dataclass(frozen=True)
class Node:
    name: str
    instance_id: str
    base_url: str

    @property
    def fault_url(self) -> str:
        parsed = urllib.parse.urlparse(self.base_url)
        host = parsed.hostname or self.base_url
        return f"http://{host}:4492"


@dataclass
class PayloadSample:
    stream: str
    start_offset: int
    end_offset: int
    payload: bytes
    written_at: datetime
    producer_id: str
    producer_epoch: int
    producer_seq: int
    payload_kind: str
    cold_confirmed: bool = False


@dataclass
class ProducerState:
    producer_id: str
    epoch: int = 1
    seq: int = 0
    last_payload: bytes | None = None
    last_seq: int | None = None
    last_stream: str | None = None
    last_end_offset: int | None = None


def node_id_from_name(name: str) -> int | None:
    try:
        return int(name.rsplit("-", 1)[-1])
    except ValueError:
        return None


def response_header(headers: dict[str, str], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value
    return None


@dataclass
class WorkloadStream:
    name: str
    next_offset: int = 0
    verified_offsets: int = 0
    expected_live_setsum: Setsum | None = None
    recent_payloads: deque[PayloadSample] | None = None
    old_payloads: deque[PayloadSample] | None = None
    producer_epochs: dict[str, int] | None = None
    producer_seqs: dict[str, int] | None = None

    def __post_init__(self) -> None:
        if self.expected_live_setsum is None:
            self.expected_live_setsum = Setsum()
        if self.recent_payloads is None:
            self.recent_payloads = deque(maxlen=2048)
        if self.old_payloads is None:
            self.old_payloads = deque(maxlen=512)
        if self.producer_epochs is None:
            self.producer_epochs = {}
        if self.producer_seqs is None:
            self.producer_seqs = {}


class ChaosAgent:
    def __init__(self, args: argparse.Namespace) -> None:
        self.nodes = [parse_node(raw) for raw in args.node]
        if not self.nodes:
            raise SystemExit("at least one --node is required")
        self.status_file = args.status_file
        self.status_s3_uri = args.status_s3_uri
        self.history: deque[dict[str, Any]] = deque(maxlen=args.history_points)
        self.append_per_second = args.append_per_second
        self.payload_bytes = args.payload_bytes
        self.payload_sizes = parse_int_list(args.payload_sizes)
        self.payload_kinds = [kind.strip() for kind in args.payload_kinds.split(",") if kind.strip()]
        self.verify_every = args.verify_every
        self.verify_modes = [mode.strip() for mode in args.verify_modes.split(",") if mode.strip()]
        self.reader_count = args.reader_count
        self.status_every = args.status_every
        self.fault_min_secs = args.fault_min_secs
        self.fault_max_secs = args.fault_max_secs
        fault_scenarios = args.fault_scenarios or FAULT_PROFILES.get(args.fault_profile, "")
        if not fault_scenarios:
            raise SystemExit("--fault-scenarios is required when --fault-profile=custom")
        self.fault_profile = args.fault_profile
        self.fault_scenarios = [scenario.strip() for scenario in fault_scenarios.split(",") if scenario.strip()]
        configured_unsupported = sorted(set(self.fault_scenarios) & UNSUPPORTED_QUORUM_LOSS_SCENARIOS)
        if configured_unsupported:
            raise SystemExit(
                "unsupported --fault-scenarios for the default chaos run: "
                + ",".join(configured_unsupported)
                + "; a 3-node --raft-memory run should not intentionally drop quorum"
            )
        self.recovery_slo_secs = args.recovery_slo_secs
        self.first_fault_secs = args.first_fault_secs
        self.recovery_secs = args.recovery_secs
        self.repair_retry_secs = max(30, args.repair_retry_secs)
        self.max_repair_attempts = max(0, args.max_repair_attempts)
        self.disable_faults = args.disable_faults
        self.timeout_secs = args.timeout_secs
        self.producer_count = max(1, args.producer_count)
        self.epoch_bump_every = args.epoch_bump_every
        self.producer_probe_every = args.producer_probe_every
        self.burst_every = args.burst_every
        self.burst_appends = args.burst_appends
        self.backpressure_probe_every = args.backpressure_probe_every
        self.backpressure_probe_bytes = max(1, args.backpressure_probe_bytes)
        self.backpressure_probe_max_appends = max(1, args.backpressure_probe_max_appends)
        self.old_sample_every = max(1, args.old_sample_every)
        self.started_at = utc_now()
        self.run_id = args.stream or f"run-{self.started_at.strftime('%Y%m%d%H%M%S')}"
        self.streams = [
            WorkloadStream(f"{self.run_id}-{index:04d}")
            for index in range(max(1, args.stream_count))
        ]
        self.producers = [
            ProducerState(f"chaos-agent-{index:03d}")
            for index in range(self.producer_count)
        ]
        self.append_success = 0
        self.append_errors = 0
        self.reader_success = 0
        self.reader_errors = 0
        self.read_availability_errors = 0
        self.burst_success = 0
        self.burst_errors = 0
        self.backpressure_probe_success = 0
        self.backpressure_probe_errors = 0
        self.producer_probe_success = 0
        self.producer_probe_errors = 0
        self.cold_flush_attempts = 0
        self.cold_flush_success = 0
        self.cold_flush_noop = 0
        self.cold_flush_errors = 0
        self.verify_attempts = 0
        self.verified_offsets = 0
        self.mismatch_count = 0
        self.setsum_mismatch_count = 0
        self.verify_counts: dict[str, int] = {mode: 0 for mode in self.verify_modes}
        self.verify_errors: dict[str, int] = {mode: 0 for mode in self.verify_modes}
        self.last_integrity_error: str | None = None
        self.last_read_availability_error: str | None = None
        self.last_integrity_check: datetime | None = None
        self.last_read_check: dict[str, Any] | None = None
        self.last_cold_flush: dict[str, Any] | None = None
        self.last_checked_expected_live_setsum: str | None = None
        self.last_server_integrity: dict[str, Any] | None = None
        self.events: deque[dict[str, Any]] = deque(maxlen=32)
        self.active_fault: dict[str, Any] | None = None
        self.active_injection_id: int | None = None
        self.last_fault: str | None = None
        self.next_fault_at = self.choose_next_fault(initial=True)
        self.injections: deque[dict[str, Any]] = deque(maxlen=args.injection_history)
        self.last_status_append_success: int | None = None
        self.last_status_append_errors: int | None = None
        self.last_status_reader_success: int | None = None
        self.last_status_reader_errors: int | None = None
        self.last_status_read_availability_errors: int | None = None
        self.last_status_backpressure_probe_success: int | None = None
        self.last_status_cold_backpressure_events: int | None = None
        self.cold_refresh_cursor = 0
        self.restored_workload_coverage: dict[str, Any] = {}
        self.next_burst_at = time.monotonic() + self.burst_every if self.burst_every > 0 else None
        self.next_backpressure_probe_at = (
            time.monotonic() + self.backpressure_probe_every if self.backpressure_probe_every > 0 else None
        )
        self.backpressure_probe_stream = f"{self.run_id}-backpressure"
        self.backpressure_probe_seq = 0
        self.restore_published_state()

    def choose_next_fault(self, *, initial: bool = False) -> datetime | None:
        if self.disable_faults:
            return None
        if initial and self.first_fault_secs is not None:
            return utc_now() + timedelta(seconds=self.first_fault_secs)
        return utc_now() + timedelta(seconds=random.randint(self.fault_min_secs, self.fault_max_secs))

    def event(self, level: str, message: str) -> None:
        self.events.appendleft({"time": iso(utc_now()), "level": level, "message": message})
        print(f"{iso(utc_now())} {level.upper()} {message}", flush=True)

    def restore_published_state(self) -> None:
        status = self.load_previous_status()
        if not status:
            return

        self.history.extend(status.get("history", []))
        self.events.extend(status.get("events", []))
        workload_coverage = status.get("workload", {}).get("coverage", {})
        if isinstance(workload_coverage, dict):
            self.restored_workload_coverage = workload_coverage
        chaos = status.get("chaos", {})
        self.last_fault = chaos.get("last_fault")
        restored_next_fault = parse_iso(chaos.get("next_fault_after"))
        if restored_next_fault is not None and restored_next_fault > utc_now():
            self.next_fault_at = restored_next_fault

        for injection in chaos.get("injections", []):
            if isinstance(injection, dict):
                self.injections.append(injection)
        if not self.injections:
            return

        latest = self.injections[-1]
        if latest.get("recovered_at") is not None:
            return
        injection_id = latest.get("id")
        if isinstance(injection_id, int):
            self.active_injection_id = injection_id

        node = next((node for node in self.nodes if node.name == latest.get("node_name")), None)
        recover_at = parse_iso(latest.get("recover_after"))
        if node is not None and latest.get("start_requested_at") is None:
            target_names = latest.get("target_nodes")
            if not isinstance(target_names, list):
                target_names = [node.name]
            self.active_fault = {
                "scenario": latest.get("scenario", "clean_stop"),
                "targets": [target for target in self.nodes if target.name in target_names],
                "recover_at": recover_at or utc_now(),
                "allow_revert": latest.get("allow_next_revert", True),
                "cleanup": latest.get("cleanup", "start_instances"),
            }

    def load_previous_status(self) -> dict[str, Any] | None:
        if not self.status_file.exists():
            return None
        try:
            return json.loads(self.status_file.read_text())
        except Exception as exc:  # noqa: BLE001
            print(f"{iso(utc_now())} WARN unable to restore previous status: {exc}", flush=True)
            return None

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

    def create_streams(self) -> None:
        for stream in self.streams:
            for node in self.nodes:
                status, _, _ = self.request("PUT", f"{node.base_url}/{BUCKET}/{stream.name}")
                if status in {200, 201, 409}:
                    break
            else:
                raise RuntimeError(f"unable to create chaos stream {stream.name} on any node")
        self.event("info", f"{len(self.streams)} streams ready for run {self.run_id}")

    def append_once(self) -> None:
        stream = self.streams[self.append_success % len(self.streams)]
        producer = self.producers[self.append_success % len(self.producers)]
        if self.epoch_bump_every > 0 and self.append_success > 0 and self.append_success % self.epoch_bump_every == 0:
            producer.epoch += 1
            for candidate in self.streams:
                candidate.producer_seqs[producer.producer_id] = 0
            self.event("info", f"{producer.producer_id} bumped epoch to {producer.epoch}")
        stream.producer_epochs[producer.producer_id] = producer.epoch
        producer_seq = stream.producer_seqs.get(producer.producer_id, 0)
        start_offset = stream.next_offset
        payload_size = self.payload_sizes[self.append_success % len(self.payload_sizes)]
        payload_kind = self.payload_kinds[self.append_success % len(self.payload_kinds)] if self.payload_kinds else "ascii"
        payload = self.build_payload(payload_size, payload_kind, stream, producer, producer_seq, start_offset)
        first_node = self.append_success % len(self.nodes)
        last_error = "no target nodes"
        for attempt in range(len(self.nodes)):
            node = self.nodes[(first_node + attempt) % len(self.nodes)]
            try:
                status, _, headers = self.request(
                    "POST",
                    f"{node.base_url}/{BUCKET}/{stream.name}",
                    body=payload,
                    headers={
                        "Content-Type": CONTENT_TYPE,
                        "Producer-Id": producer.producer_id,
                        "Producer-Epoch": str(producer.epoch),
                        "Producer-Seq": str(producer_seq),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                last_error = f"{node.name}: {exc}"
                continue
            if status not in {200, 204}:
                last_error = f"{node.name}: status={status}"
                continue
            next_offset_header = headers.get("Stream-Next-Offset")
            next_offset_value = None
            if next_offset_header is not None:
                try:
                    next_offset_value = int(next_offset_header)
                    stream.next_offset = max(stream.next_offset + len(payload), next_offset_value)
                except ValueError:
                    stream.next_offset += len(payload)
            else:
                stream.next_offset += len(payload)
            end_offset = start_offset + len(payload)
            if next_offset_value is not None:
                end_offset = next_offset_value
            stream.expected_live_setsum.insert_vectored(
                [
                    b"ursula-stream-record-v1",
                    BUCKET.encode(),
                    b"\0",
                    stream.name.encode(),
                    b"\0",
                    start_offset.to_bytes(8, "little"),
                    end_offset.to_bytes(8, "little"),
                    b"inline",
                    payload,
                ]
            )
            sample = PayloadSample(
                stream=stream.name,
                start_offset=start_offset,
                end_offset=end_offset,
                payload=payload,
                written_at=utc_now(),
                producer_id=producer.producer_id,
                producer_epoch=producer.epoch,
                producer_seq=producer_seq,
                payload_kind=payload_kind,
            )
            stream.recent_payloads.append(sample)
            if self.append_success % self.old_sample_every == 0:
                stream.old_payloads.append(sample)
            producer.last_payload = payload
            producer.last_seq = producer_seq
            producer.last_stream = stream.name
            producer.last_end_offset = end_offset
            stream.producer_seqs[producer.producer_id] = producer_seq + 1
            self.append_success += 1
            return
        self.append_errors += 1
        self.event("warn", f"append failed on all nodes: {last_error}")

    def build_payload(
        self,
        size: int,
        kind: str,
        stream: WorkloadStream,
        producer: ProducerState,
        producer_seq: int,
        start_offset: int,
    ) -> bytes:
        prefix = (
            f"{self.append_success:020d}:{stream.name}:{start_offset:020d}:"
            f"{producer.producer_id}:{producer.epoch}:{producer_seq}:{kind}\n"
        ).encode()
        if kind == "zero":
            filler = b"\0" * max(0, size - len(prefix))
        elif kind == "utf8":
            filler = ("数据-" * max(1, size // 8)).encode()
        elif kind == "binary":
            seed = hashlib.sha3_256(prefix).digest()
            filler = (seed * ((max(0, size - len(prefix)) // len(seed)) + 1))[: max(0, size - len(prefix))]
        else:
            filler = b"x" * max(0, size - len(prefix))
        return (prefix + filler)[:size]

    def verify_integrity(self) -> None:
        mode = self.verify_modes[self.verify_attempts % len(self.verify_modes)] if self.verify_modes else "latest"
        self.verify_attempts += 1
        sample = self.choose_verify_sample(mode)
        if sample is None:
            self.verify_errors[mode] = self.verify_errors.get(mode, 0) + 1
            self.last_integrity_check = utc_now()
            return
        if mode == "cold":
            if not self.ensure_cold_sample(sample):
                self.verify_errors[mode] = self.verify_errors.get(mode, 0) + 1
                self.last_integrity_check = utc_now()
                return
        last_error = self.verify_sample(sample)
        self.last_integrity_check = utc_now()
        if last_error is None:
            self.verified_offsets += 1
            self.verify_counts[mode] = self.verify_counts.get(mode, 0) + 1
            stream = next((stream for stream in self.streams if stream.name == sample.stream), None)
            if stream is not None:
                stream.verified_offsets += 1
                self.verify_server_integrity(stream)
            return
        self.verify_errors[mode] = self.verify_errors.get(mode, 0) + 1
        if self.is_read_availability_error(last_error):
            self.read_availability_errors += 1
            self.last_read_availability_error = last_error
            self.event("warn", f"{mode} read availability check failed: {last_error}")
            return
        self.mismatch_count += 1
        self.last_integrity_error = last_error
        self.event("error", f"{mode} integrity check failed: {self.last_integrity_error}")

    def choose_verify_sample(self, mode: str) -> PayloadSample | None:
        populated = [stream for stream in self.streams if stream.recent_payloads]
        if not populated:
            return None
        stream = populated[self.verify_attempts % len(populated)]
        if mode == "latest":
            return stream.recent_payloads[-1]
        if mode == "recent":
            return random.choice(list(stream.recent_payloads))
        if mode == "cold":
            self.refresh_cold_confirmed_samples(max_streams=32)
            cold_samples = [
                sample
                for candidate in self.streams
                for sample in list(candidate.old_payloads) + list(candidate.recent_payloads)
                if sample.cold_confirmed
            ]
            if cold_samples:
                return random.choice(cold_samples)
        if mode in {"old", "cold"}:
            old_streams = [stream for stream in self.streams if stream.old_payloads]
            if old_streams:
                chosen = old_streams[self.verify_attempts % len(old_streams)]
                return random.choice(list(chosen.old_payloads))
        return stream.recent_payloads[-1]

    def verify_sample(self, sample: PayloadSample) -> str | None:
        last_error: str | None = None
        node_results: list[dict[str, Any]] = []
        for node in self.nodes:
            try:
                status, body, _ = self.request(
                    "GET",
                    f"{node.base_url}/{BUCKET}/{sample.stream}?{urllib.parse.urlencode({'offset': sample.start_offset, 'max_bytes': len(sample.payload)})}",
                )
            except Exception as exc:  # noqa: BLE001
                error = f"{node.name} read failed: {exc}"
                node_results.append({"node": node.name, "status": "error", "error": str(exc)})
                last_error = error
                continue
            if status == 200 and body.startswith(sample.payload):
                self.last_read_check = {
                    "stream": sample.stream,
                    "offset": sample.start_offset,
                    "bytes": len(sample.payload),
                    "payload_kind": sample.payload_kind,
                    "matched_node": node.name,
                    "nodes": node_results + [{"node": node.name, "status": status, "matched": True}],
                }
                self.last_integrity_error = None
                return None
            body_prefix = body[:32]
            node_result: dict[str, Any] = {"node": node.name, "status": status, "matched": False}
            if body_prefix:
                node_result["body_prefix_hex"] = body_prefix.hex()
            node_results.append(node_result)
            if status in READ_AVAILABILITY_STATUSES:
                last_error = f"{node.name} read status={status}"
                continue
            last_error = f"{node.name} read status={status} body_prefix={body[:32]!r}"
        self.last_read_check = {
            "stream": sample.stream,
            "offset": sample.start_offset,
            "bytes": len(sample.payload),
            "payload_kind": sample.payload_kind,
            "nodes": node_results,
        }
        summary = "; ".join(
            f"{result['node']}={result.get('status')}"
            + (f":{result['error']}" if result.get("error") else "")
            for result in node_results
        )
        if summary:
            return f"{last_error or 'readback mismatch'} ({summary})"
        return last_error or "readback mismatch"

    def is_read_availability_error(self, error: str | None) -> bool:
        if not error:
            return False
        if "body_prefix=" in error or "body_prefix_hex" in error:
            return False
        if " read failed:" in error:
            return True
        return any(f"read status={status}" in error for status in READ_AVAILABILITY_STATUSES)

    def ensure_cold_sample(self, sample: PayloadSample) -> bool:
        if sample.cold_confirmed:
            return True
        return self.flush_cold_for_sample(sample)

    def flush_cold_for_sample(self, sample: PayloadSample) -> bool:
        self.cold_flush_attempts += 1
        query = urllib.parse.urlencode(
            {
                "min_hot_bytes": 1,
                "max_bytes": max(1, sample.end_offset - sample.start_offset),
            }
        )
        last_error = "no target nodes"
        for node in self.nodes:
            try:
                status, body, _ = self.request(
                    "POST",
                    f"{node.base_url}/__ursula/flush-cold/{BUCKET}/{sample.stream}?{query}",
                )
            except Exception as exc:  # noqa: BLE001
                last_error = f"{node.name}: {exc}"
                continue
            self.last_cold_flush = {
                "node": node.name,
                "stream": sample.stream,
                "status": status,
                "body_prefix": body[:80].decode("utf-8", errors="replace") if body else "",
            }
            if status == 200:
                self.mark_cold_confirmed_from_flush(sample.stream, body)
                self.cold_flush_success += 1
                return sample.cold_confirmed
            if status == 204:
                self.cold_flush_noop += 1
                if self.refresh_cold_confirmed_from_head(sample):
                    return True
                last_error = f"{node.name}: no cold flush candidate and sample is not below live start"
                continue
            last_error = f"{node.name}: status={status} body={body[:80]!r}"
        self.cold_flush_errors += 1
        self.last_cold_flush = {
            "stream": sample.stream,
            "status": "error",
            "error": last_error,
        }
        self.event("warn", f"cold flush before verify failed: {last_error}")
        return False

    def mark_cold_confirmed_from_flush(self, stream_name: str, body: bytes) -> None:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return
        hot_start_offset = parse_int(str(payload.get("hot_start_offset"))) if isinstance(payload, dict) else None
        if hot_start_offset is None:
            return
        stream = next((stream for stream in self.streams if stream.name == stream_name), None)
        if stream is None:
            return
        for sample in list(stream.old_payloads) + list(stream.recent_payloads):
            if sample.end_offset <= hot_start_offset:
                sample.cold_confirmed = True

    def refresh_cold_confirmed_from_head(self, sample: PayloadSample) -> bool:
        stream = next((stream for stream in self.streams if stream.name == sample.stream), None)
        if stream is None:
            return False
        return self.refresh_stream_cold_confirmed(stream) and sample.cold_confirmed

    def refresh_cold_confirmed_samples(self, *, max_streams: int) -> None:
        streams = [stream for stream in self.streams if stream.old_payloads or stream.recent_payloads]
        if not streams:
            return
        max_streams = max(1, min(max_streams, len(streams)))
        start = self.cold_refresh_cursor % len(streams)
        self.cold_refresh_cursor = (start + max_streams) % len(streams)
        for index in range(max_streams):
            stream = streams[(start + index) % len(streams)]
            self.refresh_stream_cold_confirmed(stream)

    def refresh_stream_cold_confirmed(self, stream: WorkloadStream) -> bool:
        for node in self.nodes:
            try:
                status, _, headers = self.request("HEAD", f"{node.base_url}/{BUCKET}/{stream.name}")
            except Exception:
                continue
            if status != 200:
                continue
            header_map = {key.lower(): value for key, value in headers.items()}
            cold_hot_start_offset = parse_int(header_map.get("stream-cold-hot-start-offset"))
            if cold_hot_start_offset is None:
                continue
            for candidate in list(stream.old_payloads) + list(stream.recent_payloads):
                if candidate.end_offset <= cold_hot_start_offset:
                    candidate.cold_confirmed = True
            return True
        return False

    def run_reader_probe(self) -> None:
        for _ in range(self.reader_count):
            mode = random.choice(self.verify_modes or ["recent"])
            sample = self.choose_verify_sample(mode)
            if sample is None:
                return
            error = self.verify_sample(sample)
            if error is None:
                self.reader_success += 1
            else:
                self.reader_errors += 1
                availability_error = self.is_read_availability_error(error)
                level = "warn" if availability_error else "error"
                if availability_error:
                    self.read_availability_errors += 1
                    self.last_read_availability_error = error
                if level == "error":
                    self.last_integrity_error = error
                self.event(level, f"reader {mode} failed: {error}")

    def record_producer_probe_result(self, ok: bool, message: str) -> None:
        if ok:
            self.producer_probe_success += 1
        else:
            self.producer_probe_errors += 1
            self.event("warn", message)

    def run_producer_semantics_probe(self) -> None:
        candidates = [producer for producer in self.producers if producer.last_payload is not None and producer.last_stream]
        if not candidates:
            return
        producer = candidates[self.producer_probe_success % len(candidates)]
        node = self.nodes[(self.producer_probe_success + self.producer_probe_errors) % len(self.nodes)]
        stale_epoch = max(0, producer.epoch - 1)
        probes = [
            ("duplicate_seq", producer.epoch, producer.last_seq, producer.last_payload),
            ("stale_epoch", stale_epoch, producer.last_seq, producer.last_payload),
        ]
        for kind, epoch, seq, payload in probes:
            if seq is None or payload is None:
                continue
            try:
                status, _, headers = self.request(
                    "POST",
                    f"{node.base_url}/{BUCKET}/{producer.last_stream}",
                    body=payload,
                    headers={
                        "Content-Type": CONTENT_TYPE,
                        "Producer-Id": producer.producer_id,
                        "Producer-Epoch": str(epoch),
                        "Producer-Seq": str(seq),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                self.record_producer_probe_result(False, f"producer {kind} probe failed: {exc}")
                continue
            if kind == "duplicate_seq":
                next_offset = parse_int(response_header(headers, "Stream-Next-Offset"))
                self.record_producer_probe_result(
                    status == 204 and next_offset == producer.last_end_offset,
                    (
                        "producer duplicate_seq probe did not deduplicate: "
                        f"status={status} next_offset={next_offset} expected={producer.last_end_offset}"
                    ),
                )
                continue
            current_epoch = parse_int(response_header(headers, "Producer-Epoch"))
            self.record_producer_probe_result(
                status == 403 and current_epoch == producer.epoch,
                (
                    "producer stale_epoch probe was not fenced: "
                    f"status={status} current_epoch={current_epoch} expected={producer.epoch}"
                ),
            )

    def run_burst_probe(self) -> None:
        for _ in range(self.burst_appends):
            before_success = self.append_success
            before_errors = self.append_errors
            self.append_once()
            self.burst_success += self.append_success - before_success
            self.burst_errors += self.append_errors - before_errors

    def workload_probes_paused(self) -> bool:
        if self.active_fault is not None:
            return True
        injection = self.current_injection()
        return injection is not None and injection.get("recovered_at") is None

    def run_backpressure_probe(self) -> None:
        if self.workload_probes_paused():
            return
        stream_name = self.backpressure_probe_stream
        producer_id = "chaos-agent-backpressure"
        for node in self.nodes:
            try:
                status, _, _ = self.request("PUT", f"{node.base_url}/{BUCKET}/{stream_name}")
            except Exception:
                continue
            if status in {200, 201, 409}:
                break
        payload = b"b" * self.backpressure_probe_bytes
        last_error = "no target nodes"
        for attempt in range(self.backpressure_probe_max_appends):
            node = self.nodes[(self.backpressure_probe_seq + attempt) % len(self.nodes)]
            producer_seq = self.backpressure_probe_seq
            try:
                status, body, _ = self.request(
                    "POST",
                    f"{node.base_url}/{BUCKET}/{stream_name}",
                    body=payload,
                    headers={
                        "Content-Type": CONTENT_TYPE,
                        "Producer-Id": producer_id,
                        "Producer-Epoch": "1",
                        "Producer-Seq": str(producer_seq),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                last_error = f"{node.name}: {exc}"
                continue
            body_text = body[:160].decode("utf-8", errors="replace")
            if status == 503 and "ColdBackpressure" in body_text:
                self.backpressure_probe_success += 1
                return
            if status in {200, 204}:
                self.backpressure_probe_seq += 1
                continue
            last_error = f"{node.name}: status={status} body={body_text!r}"
            if status not in READ_AVAILABILITY_STATUSES:
                break
        self.backpressure_probe_errors += 1
        self.event("warn", f"backpressure probe did not observe ColdBackpressure: {last_error}")

    def verify_server_integrity(self, stream: WorkloadStream) -> None:
        expected = stream.expected_live_setsum.hexdigest()
        last_error: str | None = None
        for node in self.nodes:
            try:
                status, _, headers = self.request("HEAD", f"{node.base_url}/{BUCKET}/{stream.name}")
            except Exception as exc:  # noqa: BLE001
                last_error = f"{node.name} head failed: {exc}"
                continue
            if status != 200:
                last_error = f"{node.name} head status={status}"
                continue
            header_map = {key.lower(): value for key, value in headers.items()}
            server_live = header_map.get("stream-integrity-live-setsum")
            server_total = header_map.get("stream-integrity-total-setsum")
            server_evicted_records = header_map.get("stream-integrity-evicted-records")
            live_start_offset = parse_int(header_map.get("stream-integrity-live-start-offset"))
            live_records = parse_int(header_map.get("stream-integrity-live-records"))
            total_records = parse_int(header_map.get("stream-integrity-total-records"))
            evicted_records = parse_int(server_evicted_records)
            recomputed_live = self.recompute_live_setsum(stream, live_start_offset, live_records)
            self.last_checked_expected_live_setsum = expected
            self.last_server_integrity = {
                "node": node.name,
                "stream": stream.name,
                "expected_live_setsum": expected,
                "recomputed_live_setsum": recomputed_live,
                "live_setsum": server_live,
                "total_setsum": server_total,
                "evicted_records": evicted_records,
                "live_start_offset": live_start_offset,
                "live_records": live_records,
                "total_records": total_records,
            }
            if server_live == expected and server_total == expected and server_evicted_records == "0":
                self.last_integrity_error = None
                return
            if evicted_records and evicted_records > 0:
                if recomputed_live is None:
                    self.last_integrity_error = None
                    self.last_server_integrity["check"] = "eviction-aware-skip-missing-history"
                    return
                if server_live == recomputed_live:
                    self.last_integrity_error = None
                    self.last_server_integrity["check"] = "eviction-aware-live-match"
                    return
            self.setsum_mismatch_count += 1
            self.last_integrity_error = (
                f"{node.name} setsum mismatch expected={expected} recomputed_live={recomputed_live} "
                f"live={server_live} total={server_total} evicted_records={server_evicted_records}"
            )
            self.event("error", f"integrity setsum failed: {self.last_integrity_error}")
            return
        self.setsum_mismatch_count += 1
        self.last_integrity_error = last_error or "server integrity headers unavailable"
        self.event("error", f"integrity setsum failed: {self.last_integrity_error}")

    def recompute_live_setsum(
        self,
        stream: WorkloadStream,
        live_start_offset: int | None,
        live_records: int | None,
    ) -> str | None:
        if live_start_offset is None or live_records is None:
            return None
        samples = [
            sample
            for sample in list(stream.old_payloads) + list(stream.recent_payloads)
            if sample.start_offset >= live_start_offset
        ]
        by_offset = {sample.start_offset: sample for sample in samples}
        unique = [by_offset[offset] for offset in sorted(by_offset)]
        if len(unique) < live_records:
            return None
        live = Setsum()
        for sample in unique[-live_records:]:
            live.insert_vectored(
                [
                    b"ursula-stream-record-v1",
                    BUCKET.encode(),
                    b"\0",
                    stream.name.encode(),
                    b"\0",
                    sample.start_offset.to_bytes(8, "little"),
                    sample.end_offset.to_bytes(8, "little"),
                    b"inline",
                    sample.payload,
                ]
            )
        return live.hexdigest()

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
                sample["cold_hot_bytes"] = metrics.get("cold_hot_bytes")
                sample["cold_hot_group_bytes_max"] = metrics.get("cold_hot_group_bytes_max")
                sample["cold_hot_stream_bytes_max"] = metrics.get("cold_hot_stream_bytes_max")
                sample["cold_flush_uploads"] = metrics.get("cold_flush_uploads")
                sample["cold_flush_upload_bytes"] = metrics.get("cold_flush_upload_bytes")
                sample["cold_flush_publishes"] = metrics.get("cold_flush_publishes")
                sample["cold_flush_publish_bytes"] = metrics.get("cold_flush_publish_bytes")
                sample["cold_backpressure_events"] = metrics.get("cold_backpressure_events")
                sample["cold_backpressure_bytes"] = metrics.get("cold_backpressure_bytes")
                sample["cold_store"] = metrics.get("cold_store")
                sample["raft_groups"] = len(raft_groups)
                sample["leader_groups"] = sum(1 for group in raft_groups if group.get("current_leader") is not None)
                sample["node_id"] = raft_groups[0].get("node_id") if raft_groups else None
                sample["raft_group_states"] = [
                    {
                        "raft_group_id": group.get("raft_group_id"),
                        "node_id": group.get("node_id"),
                        "current_leader": group.get("current_leader"),
                        "voter_ids": group.get("voter_ids", []),
                        "learner_ids": group.get("learner_ids", []),
                        "committed_index": group.get("committed_index"),
                        "last_applied_index": group.get("last_applied_index"),
                    }
                    for group in raft_groups
                ]
        except Exception as exc:  # noqa: BLE001
            sample["metrics_state"] = "unavailable"
            sample["last_error"] = str(exc)
        return sample

    def build_topology(self, nodes: list[dict[str, Any]]) -> dict[str, Any]:
        node_names_by_id = {
            node["node_id"]: node["name"]
            for node in nodes
            if isinstance(node.get("node_id"), int)
        }
        groups: dict[int, dict[str, Any]] = {}
        for node in nodes:
            for state in node.get("raft_group_states", []):
                group_id = state.get("raft_group_id")
                if not isinstance(group_id, int):
                    continue
                group = groups.setdefault(
                    group_id,
                    {
                        "raft_group_id": group_id,
                        "leader_id": state.get("current_leader"),
                        "leader_name": node_names_by_id.get(state.get("current_leader")),
                        "voter_ids": state.get("voter_ids", []),
                        "voter_names": [
                            node_names_by_id.get(voter_id, str(voter_id))
                            for voter_id in state.get("voter_ids", [])
                        ],
                        "learner_ids": state.get("learner_ids", []),
                        "replicas": [],
                    },
                )
                if group.get("leader_id") is None and state.get("current_leader") is not None:
                    group["leader_id"] = state.get("current_leader")
                    group["leader_name"] = node_names_by_id.get(state.get("current_leader"))
                group["replicas"].append(
                    {
                        "node_id": state.get("node_id"),
                        "node_name": node.get("name"),
                        "role": "leader" if state.get("node_id") == group.get("leader_id") else "voter",
                        "committed_index": state.get("committed_index"),
                        "last_applied_index": state.get("last_applied_index"),
                    }
                )
        return {
            "nodes": [
                {
                    "node_id": node.get("node_id"),
                    "name": node.get("name"),
                    "instance_state": node.get("instance_state"),
                    "metrics_state": node.get("metrics_state"),
                }
                for node in nodes
            ],
            "raft_groups": [groups[group_id] for group_id in sorted(groups)],
        }

    def allow_next_revert_for_node(self, target: Node) -> None:
        samples = [self.sample_node(node) for node in self.nodes]
        nodes_by_id = {
            sample.get("node_id"): node
            for sample, node in zip(samples, self.nodes)
            if isinstance(sample.get("node_id"), int)
        }
        target_sample = next((sample for sample in samples if sample.get("name") == target.name), {})
        target_id = target_sample.get("node_id")
        if not isinstance(target_id, int):
            target_id = node_id_from_name(target.name)
        if not isinstance(target_id, int):
            self.event("warn", f"skip allow-next-revert for {target.name}: unknown node id")
            return

        group_leaders: dict[int, int | None] = {}
        for sample in samples:
            for state in sample.get("raft_group_states", []):
                group_id = state.get("raft_group_id")
                if not isinstance(group_id, int):
                    continue
                leader_id = state.get("current_leader")
                if isinstance(leader_id, int):
                    group_leaders[group_id] = leader_id
                else:
                    group_leaders.setdefault(group_id, None)
        if not group_leaders:
            self.event("warn", f"skip allow-next-revert for {target.name}: no Raft groups observed")
            return

        failed_groups: list[int] = []
        for group_id, leader_id in sorted(group_leaders.items()):
            last_error = "no reachable leader observed"
            preferred_nodes = []
            leader = nodes_by_id.get(leader_id)
            if leader is not None:
                preferred_nodes.append(leader)
            preferred_nodes.extend(node for node in self.nodes if node not in preferred_nodes)
            allowed = False
            for node in preferred_nodes:
                try:
                    status, body, _ = self.request(
                        "POST",
                        f"{node.base_url}/__ursula/raft/{group_id}/nodes/{target_id}/allow-next-revert",
                    )
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    continue
                if status == 200:
                    allowed = True
                    break
                last_error = f"status={status} body={body[:80]!r}"
            if not allowed:
                failed_groups.append(group_id)
                self.event(
                    "warn",
                    f"allow-next-revert failed for {target.name} group {group_id} via leader {leader_id}: {last_error}",
                )

        if failed_groups:
            self.event(
                "warn",
                f"allowed next revert for {target.name} on {len(group_leaders) - len(failed_groups)}/{len(group_leaders)} groups",
            )
        else:
            self.event("info", f"allowed next revert for {target.name} on {len(group_leaders)} Raft groups")

    def wait_for_node_metrics(self, target: Node, *, timeout_secs: int = 90) -> bool:
        deadline = time.monotonic() + timeout_secs
        last_error = "not attempted"
        while time.monotonic() < deadline:
            try:
                status, _, _ = self.request("GET", f"{target.base_url}/__ursula/metrics")
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
            else:
                if status == 200:
                    return True
                last_error = f"status={status}"
            time.sleep(5)
        self.event("warn", f"{target.name} metrics did not become reachable before allow-next-revert: {last_error}")
        return False

    def instance_state(self, node: Node) -> str:
        try:
            return json.loads(
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
        except Exception as exc:  # noqa: BLE001
            self.event("warn", f"describe {node.name} failed during recovery check: {exc}")
            return "unknown"

    def stop_instances(self, targets: list[Node], *, wait: bool) -> None:
        if not targets:
            return
        instance_ids = [node.instance_id for node in targets]
        run(["aws", "ec2", "stop-instances", "--instance-ids", *instance_ids], check=False)
        if wait:
            run(["aws", "ec2", "wait", "instance-stopped", "--instance-ids", *instance_ids], check=False)

    def recover_stopped_nodes_on_startup(self) -> None:
        for node in self.nodes:
            state = self.instance_state(node)
            if state not in {"stopped", "stopping"}:
                continue
            self.event("warn", f"{node.name} is {state} on agent startup; starting it before workload setup")
            deadline = time.monotonic() + max(300, self.recovery_secs * 2)
            while state == "stopping" and time.monotonic() < deadline:
                time.sleep(5)
                state = self.instance_state(node)
            if state == "stopped":
                run(["aws", "ec2", "start-instances", "--instance-ids", node.instance_id], check=False)
            while time.monotonic() < deadline:
                state = self.instance_state(node)
                if state == "running":
                    break
                time.sleep(5)

    def create_streams_until_ready(self) -> None:
        while True:
            self.recover_stopped_nodes_on_startup()
            try:
                self.create_streams()
                return
            except Exception as exc:  # noqa: BLE001
                self.event("warn", f"stream setup not ready: {exc}")
                self.publish_status()
                time.sleep(max(5, min(30, self.status_every)))

    def maybe_inject_fault(self) -> None:
        now = utc_now()
        if self.disable_faults:
            return
        if self.active_fault is not None:
            recover_at = self.active_fault["recover_at"]
            if now < recover_at:
                return
            targets: list[Node] = self.active_fault["targets"]
            scenario = self.active_fault["scenario"]
            self.event("warn", f"recovering {scenario} fault on {', '.join(node.name for node in targets)}")
            if self.active_fault.get("cleanup") == "start_instances":
                if self.active_fault.get("allow_revert", False):
                    for node in targets:
                        self.allow_next_revert_for_node(node)
                run(["aws", "ec2", "start-instances", "--instance-ids", *[node.instance_id for node in targets]], check=False)
            else:
                for node in targets:
                    self.clear_node_impairment(node)
            self.last_fault = f"{scenario} on {', '.join(node.name for node in targets)}"
            injection = self.current_injection()
            if injection is not None and injection.get("start_requested_at") is None:
                injection["status"] = "starting"
                injection["start_requested_at"] = iso(now)
                injection["timeline"].append(
                    {"time": iso(now), "status": "starting", "message": f"recovery requested for {scenario}"}
                )
            self.active_fault = None
            self.next_fault_at = self.choose_next_fault()
            self.publish_status()
            return
        if self.repair_unrecovered_injection(now):
            return
        if self.next_fault_at is None or now < self.next_fault_at:
            return
        scenario = self.choose_fault_scenario()
        targets = self.choose_fault_targets(scenario)
        allow_revert = scenario in {"clean_stop", "mixed_allow_stop", "rolling_restart"} or (
            scenario == "mixed_stop" and random.choice([True, False])
        )
        injection_id = (self.injections[-1]["id"] + 1) if self.injections else 1
        self.active_injection_id = injection_id
        cleanup = "clear_impairment" if scenario.startswith("netem") or scenario == "asymmetric_partition" else "start_instances"
        self.injections.append(
            {
                "id": injection_id,
                "scenario": scenario,
                "allow_next_revert": allow_revert,
                "expected_result": "revert_detection" if scenario in REVERT_DETECTION_SCENARIOS else "recovery",
                "node_id": targets[0].name.rsplit("-", 1)[-1],
                "node_name": targets[0].name,
                "target_nodes": [node.name for node in targets],
                "cleanup": cleanup,
                "recovery_slo_secs": self.recovery_slo_secs,
                "status": "stopping",
                "stop_requested_at": iso(now),
                "stopped_at": None,
                "start_requested_at": None,
                "recovered_at": None,
                "recover_after": iso(now + timedelta(seconds=self.recovery_secs)),
                "timeline": [
                    {
                        "time": iso(now),
                        "status": "stopping",
                        "message": f"{scenario} requested for {', '.join(node.name for node in targets)}",
                    }
                ],
            }
        )
        self.active_fault = {
            "scenario": scenario,
            "targets": targets,
            "recover_at": now + timedelta(seconds=self.recovery_secs),
            "allow_revert": allow_revert,
            "cleanup": cleanup,
        }
        self.publish_status()
        self.event("warn", f"injecting {scenario} on {', '.join(node.name for node in targets)}")
        self.apply_fault_scenario(scenario, targets, allow_revert=allow_revert)
        injection = self.current_injection()
        if injection is not None and cleanup == "clear_impairment":
            injected_at = iso(utc_now())
            applied = injection.get("fault_apply_ok") is not False
            injection["status"] = "injected" if applied else "inject_failed"
            injection["injected_at"] = injected_at
            injection["timeline"].append(
                {
                    "time": injected_at,
                    "status": "injected" if applied else "inject_failed",
                    "message": (
                        f"{scenario} active on {', '.join(node.name for node in targets)}"
                        if applied
                        else f"{scenario} failed to apply on {', '.join(node.name for node in targets)}"
                    ),
                }
            )
        self.publish_status()

    def repair_unrecovered_injection(self, now: datetime) -> bool:
        injection = self.current_injection()
        if injection is None or injection.get("recovered_at") is not None:
            return False
        if injection.get("start_requested_at") is None:
            return False
        if injection.get("slo_missed_at") is None:
            return True

        repair_count = int(injection.get("repair_attempts") or 0)
        if repair_count >= self.max_repair_attempts:
            if injection.get("status") != "repair_failed":
                injection["status"] = "repair_failed"
                injection["repair_failed_at"] = iso(now)
                injection["timeline"].append(
                    {
                        "time": iso(now),
                        "status": "repair_failed",
                        "message": f"repair stopped after {repair_count} attempts; pausing further fault injection",
                    }
                )
                self.active_injection_id = None
                self.next_fault_at = None
                self.publish_status()
            return True

        repair_requested_at = parse_iso(injection.get("repair_requested_at"))
        if repair_requested_at is not None:
            if self.active_fault is not None:
                return True
            next_retry_at = repair_requested_at + timedelta(seconds=self.repair_retry_secs)
            if now < next_retry_at:
                return True

        target_names = injection.get("target_nodes")
        if not isinstance(target_names, list) or not target_names:
            target_names = [injection.get("node_name")]
        targets = [node for node in self.nodes if node.name in set(target_names)]
        if not targets:
            injection["repair_requested_at"] = iso(now)
            injection["timeline"].append(
                {
                    "time": iso(now),
                    "status": "repair_failed",
                    "message": "repair skipped: no target nodes found",
                }
            )
            return True

        injection["status"] = "repairing"
        repair_count += 1
        injection["repair_requested_at"] = iso(now)
        injection["repair_attempts"] = repair_count
        target_label = ", ".join(node.name for node in targets)
        if injection.get("cleanup") == "clear_impairment":
            for node in targets:
                self.clear_node_impairment(node)
            injection["timeline"].append(
                {
                    "time": iso(now),
                    "status": "repairing",
                    "message": (
                        f"recovery missed SLO; repair attempt {repair_count} is clearing impairment on {target_label}"
                    ),
                }
            )
        else:
            self.stop_instances(targets, wait=True)
            injection["timeline"].append(
                {
                    "time": iso(now),
                    "status": "repairing",
                    "message": (
                        f"recovery missed SLO; repair attempt {repair_count} is restarting {target_label}; "
                        "log revert will be allowed after target metrics are reachable"
                    ),
                }
            )
            self.active_fault = {
                "scenario": f"repair_{injection.get('scenario', 'fault')}",
                "targets": targets,
                "recover_at": now + timedelta(seconds=30),
                "allow_revert": True,
                "cleanup": "start_instances",
            }
        self.publish_status()
        return True

    def choose_fault_scenario(self) -> str:
        scenario = self.fault_scenarios[(self.injections[-1]["id"] if self.injections else 0) % len(self.fault_scenarios)]
        if scenario == "mixed_stop":
            return "mixed_stop"
        return scenario

    def choose_fault_targets(self, scenario: str) -> list[Node]:
        return [random.choice(self.nodes)]

    def apply_fault_scenario(self, scenario: str, targets: list[Node], *, allow_revert: bool = False) -> None:
        if scenario in {"clean_stop", "no_allow_stop", "mixed_stop", "rolling_restart"}:
            self.stop_instances(targets, wait=allow_revert)
            return
        if scenario == "netem_delay":
            applied = True
            for node in targets:
                applied = self.apply_node_impairment(
                    node,
                    {"kind": "netem", "delay_ms": 250, "jitter_ms": 75, "loss_percent": 0},
                ) and applied
            self.mark_current_injection_apply_result(applied)
            return
        if scenario == "netem_loss":
            applied = True
            for node in targets:
                applied = self.apply_node_impairment(
                    node,
                    {"kind": "netem", "delay_ms": 0, "jitter_ms": 0, "loss_percent": 15},
                ) and applied
            self.mark_current_injection_apply_result(applied)
            return
        if scenario == "asymmetric_partition":
            peers = [urllib.parse.urlparse(node.base_url).hostname for node in self.nodes if node not in targets]
            applied = True
            for node in targets:
                applied = self.apply_node_impairment(node, {"kind": "partition", "peer_hosts": peers}) and applied
            self.mark_current_injection_apply_result(applied)
            return
        self.event("warn", f"unknown fault scenario {scenario}; falling back to clean stop")
        run(["aws", "ec2", "stop-instances", "--instance-ids", *[node.instance_id for node in targets]], check=False)

    def mark_current_injection_apply_result(self, applied: bool) -> None:
        injection = self.current_injection()
        if injection is not None:
            injection["fault_apply_ok"] = applied

    def apply_node_impairment(self, node: Node, payload: dict[str, Any]) -> bool:
        try:
            status, body, _ = self.request(
                "POST",
                f"{node.fault_url}/apply",
                body=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
        except Exception as exc:  # noqa: BLE001
            self.event("warn", f"faultd apply failed on {node.name}: {exc}")
            return False
        if status != 200:
            self.event("warn", f"faultd apply failed on {node.name}: status={status} body={body[:80]!r}")
            return False
        return True

    def clear_node_impairment(self, node: Node) -> None:
        try:
            status, body, _ = self.request("POST", f"{node.fault_url}/clear")
        except Exception as exc:  # noqa: BLE001
            self.event("warn", f"faultd clear failed on {node.name}: {exc}")
            return
        if status != 200:
            self.event("warn", f"faultd clear failed on {node.name}: status={status} body={body[:80]!r}")

    def current_injection(self) -> dict[str, Any] | None:
        if self.active_injection_id is None:
            return None
        for injection in reversed(self.injections):
            if injection.get("id") == self.active_injection_id:
                return injection
        return None

    def chaos_coverage(self) -> dict[str, Any]:
        scenarios: dict[str, dict[str, Any]] = {}
        for scenario in self.fault_scenarios:
            scenarios[scenario] = {
                "configured": True,
                "attempts": 0,
                "recovered": 0,
                "detected": 0,
                "failed": 0,
                "active": 0,
                "last_status": None,
                "last_run_at": None,
            }
        for injection in self.injections:
            scenario = str(injection.get("scenario") or "unknown")
            entry = scenarios.setdefault(
                scenario,
                {
                    "configured": False,
                    "attempts": 0,
                    "recovered": 0,
                    "detected": 0,
                    "failed": 0,
                    "active": 0,
                    "last_status": None,
                    "last_run_at": None,
                },
            )
            entry["attempts"] += 1
            status = str(injection.get("status") or "unknown")
            entry["last_status"] = status
            entry["last_run_at"] = injection.get("stop_requested_at")
            detected = injection.get("expected_result") == "revert_detection" and (
                injection.get("slo_missed_at") is not None
                or any(
                    isinstance(event, dict) and event.get("status") == "detected"
                    for event in injection.get("timeline", [])
                )
            )
            if detected:
                entry["detected"] += 1
            if status == "recovered":
                entry["recovered"] += 1
            elif status in {"inject_failed", "slo_missed"} or injection.get("fault_apply_ok") is False:
                entry["failed"] += 1
            elif status in {"stopping", "stopped", "injected", "starting", "repairing"}:
                entry["active"] += 1
        pending = sorted(scenario for scenario, entry in scenarios.items() if entry["configured"] and entry["attempts"] == 0)
        return {
            "scenario_count": len(scenarios),
            "configured_count": len(self.fault_scenarios),
            "covered_count": sum(1 for entry in scenarios.values() if entry["attempts"] > 0),
            "pending": pending,
            "scenarios": scenarios,
        }

    def workload_coverage(self, storage: dict[str, Any] | None = None) -> dict[str, Any]:
        storage = storage or {}
        cold_backpressure_events = storage.get("cold_backpressure_events")
        cold_backpressure_bytes = storage.get("cold_backpressure_bytes")
        cold_flush_publishes = storage.get("cold_flush_publishes")
        cold_flush_uploads = storage.get("cold_flush_uploads")
        cold_verify_checks = self.verify_counts.get("cold", 0)
        verify_modes = {
            mode: {
                "checks": self.verify_counts.get(mode, 0),
                "errors": self.verify_errors.get(mode, 0),
                "covered": self.verify_counts.get(mode, 0) > 0,
            }
            for mode in self.verify_modes
        }
        payload_sizes = {
            str(size): {
                "configured": True,
                "covered": self.append_success >= index + 1,
            }
            for index, size in enumerate(self.payload_sizes)
        }
        payload_kinds = {
            kind: {
                "configured": True,
                "covered": self.append_success >= index + 1,
            }
            for index, kind in enumerate(self.payload_kinds)
        }
        probes = {
            "reader": {
                "success": self.reader_success,
                "errors": self.reader_errors,
                "covered": self.reader_success + self.reader_errors > 0,
            },
            "read_availability": {
                "attempts": self.reader_success + self.reader_errors + self.verified_offsets,
                "errors": self.read_availability_errors,
                "covered": self.reader_success + self.reader_errors + self.verified_offsets > 0,
                "passing": self.read_availability_errors == 0,
            },
            "producer_semantics": {
                "success": self.producer_probe_success,
                "errors": self.producer_probe_errors,
                "covered": self.producer_probe_success + self.producer_probe_errors > 0,
                "passing": self.producer_probe_errors == 0,
            },
            "cold_flush": {
                "attempts": self.cold_flush_attempts,
                "success": self.cold_flush_success,
                "noop": self.cold_flush_noop,
                "errors": self.cold_flush_errors,
                "background_uploads": cold_flush_uploads,
                "background_publishes": cold_flush_publishes,
                "covered": self.cold_flush_success > 0
                or (isinstance(cold_flush_publishes, int) and cold_flush_publishes > 0 and cold_verify_checks > 0),
                "attempted": self.cold_flush_attempts > 0
                or (isinstance(cold_flush_uploads, int) and cold_flush_uploads > 0),
                "passing": self.cold_flush_errors == 0,
            },
            "burst": {
                "success": self.burst_success,
                "errors": self.burst_errors,
                "covered": self.burst_success + self.burst_errors > 0,
                "passing": self.burst_errors == 0,
            },
            "cold_write_backpressure": {
                "enabled": self.backpressure_probe_every > 0,
                "events": cold_backpressure_events,
                "bytes": cold_backpressure_bytes,
                "probe_success": self.backpressure_probe_success,
                "probe_errors": self.backpressure_probe_errors,
                "covered": self.backpressure_probe_success > 0
                or (isinstance(cold_backpressure_events, int) and cold_backpressure_events > 0),
                "passing": self.backpressure_probe_every <= 0
                or self.backpressure_probe_success > 0
                or self.backpressure_probe_errors == 0,
            },
        }
        coverage = {
            "verify_modes": verify_modes,
            "payload_sizes": payload_sizes,
            "payload_kinds": payload_kinds,
            "probes": probes,
            "covered_verify_mode_count": sum(1 for mode in verify_modes.values() if mode["covered"]),
            "configured_verify_mode_count": len(verify_modes),
            "covered_probe_count": sum(1 for probe in probes.values() if probe["covered"]),
            "probe_count": len(probes),
        }
        self.merge_restored_workload_coverage(coverage)
        return coverage

    def merge_restored_workload_coverage(self, coverage: dict[str, Any]) -> None:
        restored = self.restored_workload_coverage
        if not restored:
            return

        for section in ("probes", "verify_modes", "payload_sizes", "payload_kinds"):
            restored_entries = restored.get(section)
            current_entries = coverage.get(section)
            if not isinstance(restored_entries, dict) or not isinstance(current_entries, dict):
                continue
            for key, restored_entry in restored_entries.items():
                if not isinstance(restored_entry, dict) or not restored_entry.get("covered"):
                    continue
                current_entry = current_entries.setdefault(key, {})
                if not isinstance(current_entry, dict):
                    continue
                current_entry["covered"] = True
                current_entry["previously_covered"] = True
                if "passing" in restored_entry:
                    current_entry["passing"] = bool(
                        current_entry.get("passing", True) and restored_entry.get("passing", True)
                    )
                for metric in (
                    "success",
                    "probe_success",
                    "events",
                    "checks",
                    "attempts",
                    "background_publishes",
                    "background_uploads",
                ):
                    restored_value = restored_entry.get(metric)
                    current_value = current_entry.get(metric)
                    if isinstance(restored_value, int) and (
                        not isinstance(current_value, int) or current_value < restored_value
                    ):
                        current_entry[metric] = restored_value

        probes = coverage.get("probes")
        if isinstance(probes, dict):
            coverage["covered_probe_count"] = sum(
                1 for probe in probes.values() if isinstance(probe, dict) and probe.get("covered")
            )
            coverage["probe_count"] = len(probes)
        verify_modes = coverage.get("verify_modes")
        if isinstance(verify_modes, dict):
            coverage["covered_verify_mode_count"] = sum(
                1 for mode in verify_modes.values() if isinstance(mode, dict) and mode.get("covered")
            )
            coverage["configured_verify_mode_count"] = len(verify_modes)

    def raft_node_has_full_view(
        self,
        node: dict[str, Any],
        *,
        expected_groups: int,
        expected_voters: set[int],
    ) -> bool:
        if (
            expected_groups <= 0
            or node.get("metrics_state") != "ok"
            or node.get("raft_groups") != expected_groups
        ):
            return False
        states = node.get("raft_group_states", [])
        if len(states) != expected_groups:
            return False
        for state in states:
            if state.get("current_leader") is None:
                return False
            if set(state.get("voter_ids", [])) != expected_voters:
                return False
            if state.get("committed_index") is None or state.get("last_applied_index") is None:
                return False
        return True

    def build_status(self) -> dict[str, Any]:
        nodes = [self.sample_node(node) for node in self.nodes]
        topology = self.build_topology(nodes)
        expected_nodes = len(self.nodes)
        expected_voters = {node_id for node_id in (node.get("node_id") for node in nodes) if isinstance(node_id, int)}
        expected_groups = max(
            (node.get("raft_groups") for node in nodes if isinstance(node.get("raft_groups"), int)),
            default=0,
        )
        running_nodes = sum(1 for node in nodes if node.get("instance_state") == "running")
        metrics_ok = sum(1 for node in nodes if node.get("metrics_state") == "ok")
        storage = self.storage_status(nodes)
        full_raft_nodes = sum(
            1
            for node in nodes
            if len(expected_voters) == expected_nodes
            and self.raft_node_has_full_view(
                node,
                expected_groups=expected_groups,
                expected_voters=expected_voters,
            )
        )
        append_success_delta = (
            None
            if self.last_status_append_success is None
            else self.append_success - self.last_status_append_success
        )
        append_error_delta = (
            None if self.last_status_append_errors is None else self.append_errors - self.last_status_append_errors
        )
        read_availability_error_delta = (
            None
            if self.last_status_read_availability_errors is None
            else self.read_availability_errors - self.last_status_read_availability_errors
        )
        cold_backpressure_events = storage.get("cold_backpressure_events")
        cold_backpressure_event_delta = (
            None
            if self.last_status_cold_backpressure_events is None or not isinstance(cold_backpressure_events, int)
            else cold_backpressure_events - self.last_status_cold_backpressure_events
        )
        backpressure_probe_success_delta = (
            None
            if self.last_status_backpressure_probe_success is None
            else self.backpressure_probe_success - self.last_status_backpressure_probe_success
        )
        cold_backpressure_expected_probe = (
            isinstance(cold_backpressure_event_delta, int)
            and cold_backpressure_event_delta > 0
            and isinstance(backpressure_probe_success_delta, int)
            and backpressure_probe_success_delta > 0
        )
        workload_progressing = self.append_success > 0 if append_success_delta is None else append_success_delta > 0
        workload_clean = append_error_delta in (None, 0)
        read_availability_clean = read_availability_error_delta in (None, 0)
        cold_backpressure_clean = cold_backpressure_event_delta in (None, 0) or cold_backpressure_expected_probe
        integrity_status = "operational" if self.last_integrity_error is None else "major_outage"

        reasons = []
        if running_nodes < expected_nodes:
            reasons.append(f"{running_nodes}/{expected_nodes} nodes running")
        if metrics_ok < expected_nodes:
            reasons.append(f"{metrics_ok}/{expected_nodes} metrics endpoints healthy")
        if full_raft_nodes < expected_nodes:
            reasons.append(f"{full_raft_nodes}/{expected_nodes} nodes have complete Raft membership and applied state")
        if not workload_progressing:
            reasons.append("append workload is not progressing")
        if not workload_clean:
            reasons.append(f"{append_error_delta} append errors since last publish")
        if not read_availability_clean:
            reasons.append(f"{read_availability_error_delta} read availability misses since last publish")
        if not cold_backpressure_clean:
            reasons.append(f"{cold_backpressure_event_delta} cold write backpressure events since last publish")
        if integrity_status != "operational":
            reasons.append(self.last_integrity_error or "integrity check failed")

        quorum_healthy = running_nodes >= 2 and metrics_ok >= 2 and full_raft_nodes >= 2
        fully_healthy = (
            running_nodes == expected_nodes
            and metrics_ok == expected_nodes
            and full_raft_nodes == expected_nodes
            and workload_progressing
            and workload_clean
            and read_availability_clean
            and cold_backpressure_clean
            and integrity_status == "operational"
        )
        if integrity_status != "operational" or running_nodes < 2 or metrics_ok < 2:
            overall = "major_outage"
        elif fully_healthy and self.active_fault is None:
            overall = "operational"
        elif quorum_healthy and workload_progressing:
            overall = "degraded_performance"
        elif running_nodes >= 2:
            overall = "partial_outage"
        else:
            overall = "major_outage"
        active_fault = None
        if self.active_fault is not None:
            targets = ", ".join(node.name for node in self.active_fault["targets"])
            active_fault = f"{self.active_fault['scenario']} on {targets} until {iso(self.active_fault['recover_at'])}"
        updated_at = iso(utc_now())
        health = {
            "expected_nodes": expected_nodes,
            "expected_raft_groups": expected_groups,
            "running_nodes": running_nodes,
            "metrics_ok": metrics_ok,
            "full_raft_nodes": full_raft_nodes,
            "append_success_delta": append_success_delta,
            "append_error_delta": append_error_delta,
            "read_availability_error_delta": read_availability_error_delta,
            "cold_backpressure_event_delta": cold_backpressure_event_delta,
            "backpressure_probe_success_delta": backpressure_probe_success_delta,
            "workload_progressing": workload_progressing,
            "workload_clean": workload_clean,
            "read_availability_clean": read_availability_clean,
            "cold_backpressure_expected_probe": cold_backpressure_expected_probe,
            "cold_backpressure_clean": cold_backpressure_clean,
            "quorum_healthy": quorum_healthy,
            "reasons": reasons,
        }
        injection = self.current_injection()
        if injection is not None:
            target_names = injection.get("target_nodes")
            if not isinstance(target_names, list) or not target_names:
                target_names = [injection.get("node_name")]
            targets = [node for node in nodes if node.get("name") in set(target_names)]
            target_down = any(
                target.get("instance_state") != "running" or target.get("metrics_state") != "ok"
                for target in targets
            )
            if injection.get("stopped_at") is None and target_down:
                injection["status"] = "stopped"
                injection["stopped_at"] = updated_at
                injection["timeline"].append(
                    {
                        "time": updated_at,
                        "status": "stopped",
                        "message": f"{', '.join(str(name) for name in target_names)} observed unavailable",
                    }
                )
            start_requested_at = parse_iso(injection.get("start_requested_at"))
            if (
                start_requested_at is not None
                and injection.get("recovered_at") is None
                and injection.get("slo_missed_at") is None
                and (utc_now() - start_requested_at).total_seconds() > self.recovery_slo_secs
            ):
                expected_revert_detection = injection.get("expected_result") == "revert_detection"
                injection["status"] = "detected" if expected_revert_detection else "slo_missed"
                injection["slo_met"] = False
                injection["slo_missed_at"] = updated_at
                injection["timeline"].append(
                    {
                        "time": updated_at,
                        "status": "detected" if expected_revert_detection else "slo_missed",
                        "message": (
                            "revert protection detected; node did not recover without allow-next-revert"
                            if expected_revert_detection
                            else f"recovery exceeded {self.recovery_slo_secs}s SLO"
                        ),
                    }
                )
            if injection.get("start_requested_at") is not None and injection.get("recovered_at") is None and fully_healthy:
                injection["status"] = "recovered"
                injection["recovered_at"] = updated_at
                stop_requested_at = parse_iso(injection.get("stop_requested_at"))
                recovery_ms = None
                outage_ms = None
                if start_requested_at is not None:
                    recovery_ms = int((utc_now() - start_requested_at).total_seconds() * 1000)
                if stop_requested_at is not None:
                    outage_ms = int((utc_now() - stop_requested_at).total_seconds() * 1000)
                injection["recovery_ms"] = recovery_ms
                injection["outage_ms"] = outage_ms
                injection["slo_met"] = (
                    injection.get("slo_missed_at") is None
                    and recovery_ms is not None
                    and recovery_ms <= self.recovery_slo_secs * 1000
                )
                injection["timeline"].append(
                    {
                        "time": updated_at,
                        "status": "recovered",
                        "message": "cluster returned to full health",
                    }
                )
                self.active_injection_id = None
        self.history.append(
            {
                "time": updated_at,
                "status": overall,
                "running_nodes": running_nodes,
                "metrics_ok": metrics_ok,
                "full_raft_nodes": full_raft_nodes,
                "append_success_delta": append_success_delta,
                "append_error_delta": append_error_delta,
                "read_availability_error_delta": read_availability_error_delta,
                "cold_backpressure_event_delta": cold_backpressure_event_delta,
                "integrity_status": integrity_status,
                "active_fault": active_fault,
                "reasons": reasons,
            }
        )
        status = {
            "schema_version": 1,
            "overall": overall,
            "started_at": iso(self.started_at),
            "updated_at": updated_at,
            "summary": f"{running_nodes}/{expected_nodes} nodes running, {metrics_ok}/{expected_nodes} metrics endpoints healthy",
            "health": health,
            "history": list(self.history),
            "topology": topology,
            "workload": {
                "append_target_per_second": self.append_per_second,
                "append_success_total": self.append_success,
                "append_error_total": self.append_errors,
                "reader_success_total": self.reader_success,
                "reader_error_total": self.reader_errors,
                "read_availability_error_total": self.read_availability_errors,
                "producer_probe_success_total": self.producer_probe_success,
                "producer_probe_error_total": self.producer_probe_errors,
                "cold_flush_attempt_total": self.cold_flush_attempts,
                "cold_flush_success_total": self.cold_flush_success,
                "cold_flush_noop_total": self.cold_flush_noop,
                "cold_flush_error_total": self.cold_flush_errors,
                "burst_success_total": self.burst_success,
                "burst_error_total": self.burst_errors,
                "backpressure_probe_success_total": self.backpressure_probe_success,
                "backpressure_probe_error_total": self.backpressure_probe_errors,
                "backpressure_probe_enabled": self.backpressure_probe_every > 0,
                "producer_count": len(self.producers),
                "payload_sizes": self.payload_sizes,
                "payload_kinds": self.payload_kinds,
                "last_append_offset": max(stream.next_offset for stream in self.streams),
                "stream": self.run_id,
                "stream_count": len(self.streams),
                "coverage": self.workload_coverage(storage),
            },
            "integrity": {
                "status": integrity_status,
                "checked_at": iso(self.last_integrity_check),
                "verified_offsets": self.verified_offsets,
                "mismatch_count": self.mismatch_count,
                "setsum_mismatch_count": self.setsum_mismatch_count,
                "verify_counts": self.verify_counts,
                "verify_errors": self.verify_errors,
                "expected_live_setsum": self.last_checked_expected_live_setsum,
                "server": self.last_server_integrity,
                "last_read": self.last_read_check,
                "last_cold_flush": self.last_cold_flush,
                "last_error": self.last_integrity_error,
                "last_read_availability_error": self.last_read_availability_error,
            },
            "storage": storage,
            "chaos": {
                "enabled": not self.disable_faults,
                "active_fault": active_fault,
                "last_fault": self.last_fault,
                "next_fault_after": iso(self.next_fault_at),
                "fault_profile": self.fault_profile,
                "fault_scenarios": self.fault_scenarios,
                "coverage": self.chaos_coverage(),
                "recovery_slo_secs": self.recovery_slo_secs,
                "injection_count": self.injections[-1]["id"] if self.injections else 0,
                "injections": list(self.injections),
            },
            "nodes": nodes,
            "events": list(self.events),
        }
        self.last_status_append_success = self.append_success
        self.last_status_append_errors = self.append_errors
        self.last_status_read_availability_errors = self.read_availability_errors
        self.last_status_backpressure_probe_success = self.backpressure_probe_success
        if isinstance(cold_backpressure_events, int):
            self.last_status_cold_backpressure_events = cold_backpressure_events
        return status

    def storage_status(self, nodes: list[dict[str, Any]]) -> dict[str, Any]:
        def numeric(node: dict[str, Any], field: str) -> int:
            value = node.get(field)
            return value if isinstance(value, int) else 0

        backends: dict[str, int] = {}
        roots: dict[str, int] = {}
        buckets: dict[str, int] = {}
        for node in nodes:
            cold_store = node.get("cold_store")
            if not isinstance(cold_store, dict):
                continue
            backend = str(cold_store.get("backend") or "unknown")
            backends[backend] = backends.get(backend, 0) + 1
            root = cold_store.get("root")
            if root:
                root = str(root)
                roots[root] = roots.get(root, 0) + 1
            bucket = cold_store.get("bucket")
            if bucket:
                bucket = str(bucket)
                buckets[bucket] = buckets.get(bucket, 0) + 1

        return {
            "backends": backends,
            "roots": roots,
            "buckets": buckets,
            "cold_hot_bytes": sum(numeric(node, "cold_hot_bytes") for node in nodes),
            "cold_hot_group_bytes_max": max((numeric(node, "cold_hot_group_bytes_max") for node in nodes), default=0),
            "cold_hot_stream_bytes_max": max((numeric(node, "cold_hot_stream_bytes_max") for node in nodes), default=0),
            "cold_flush_uploads": sum(numeric(node, "cold_flush_uploads") for node in nodes),
            "cold_flush_upload_bytes": sum(numeric(node, "cold_flush_upload_bytes") for node in nodes),
            "cold_flush_publishes": sum(numeric(node, "cold_flush_publishes") for node in nodes),
            "cold_flush_publish_bytes": sum(numeric(node, "cold_flush_publish_bytes") for node in nodes),
            "cold_backpressure_events": sum(numeric(node, "cold_backpressure_events") for node in nodes),
            "cold_backpressure_bytes": sum(numeric(node, "cold_backpressure_bytes") for node in nodes),
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
        self.create_streams_until_ready()
        self.event("info", "chaos agent started")
        last_status = 0.0
        interval = 1.0 / max(1, self.append_per_second)
        while True:
            loop_started = time.monotonic()
            self.maybe_inject_fault()
            if loop_started - last_status >= self.status_every:
                self.publish_status()
                last_status = loop_started
            self.append_once()
            if self.append_success % self.verify_every == 0:
                self.verify_integrity()
            if self.reader_count > 0:
                self.run_reader_probe()
            workload_probes_paused = self.workload_probes_paused()
            if (
                not workload_probes_paused
                and self.producer_probe_every > 0
                and self.append_success % self.producer_probe_every == 0
            ):
                self.run_producer_semantics_probe()
            if not workload_probes_paused and self.next_burst_at is not None and loop_started >= self.next_burst_at:
                self.run_burst_probe()
                self.next_burst_at = loop_started + self.burst_every
            if (
                not workload_probes_paused
                and self.next_backpressure_probe_at is not None
                and loop_started >= self.next_backpressure_probe_at
            ):
                self.run_backpressure_probe()
                self.next_backpressure_probe_at = loop_started + self.backpressure_probe_every
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
    parser.add_argument("--stream-count", type=int, default=24)
    parser.add_argument("--append-per-second", type=int, default=20)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--payload-sizes", default="128,1024,16384,65536")
    parser.add_argument("--payload-kinds", default="ascii,binary,zero,utf8")
    parser.add_argument("--producer-count", type=int, default=8)
    parser.add_argument("--epoch-bump-every", type=int, default=5000)
    parser.add_argument("--producer-probe-every", type=int, default=200)
    parser.add_argument("--reader-count", type=int, default=2)
    parser.add_argument("--verify-modes", default="latest,recent,old,cold")
    parser.add_argument("--verify-every", type=int, default=50)
    parser.add_argument("--old-sample-every", type=int, default=128)
    parser.add_argument("--burst-every", type=int, default=300)
    parser.add_argument("--burst-appends", type=int, default=200)
    parser.add_argument("--backpressure-probe-every", type=int, default=0)
    parser.add_argument("--backpressure-probe-bytes", type=int, default=65535)
    parser.add_argument("--backpressure-probe-max-appends", type=int, default=1024)
    parser.add_argument("--status-every", type=int, default=15)
    parser.add_argument("--history-points", type=int, default=5760)
    parser.add_argument("--injection-history", type=int, default=32)
    parser.add_argument("--fault-min-secs", type=int, default=900)
    parser.add_argument("--fault-max-secs", type=int, default=1800)
    parser.add_argument(
        "--fault-profile",
        choices=["network", "revert-detection", "custom"],
        default="network",
        help="Preset fault scenario set. Use custom with --fault-scenarios.",
    )
    parser.add_argument(
        "--fault-scenarios",
        default=None,
    )
    parser.add_argument("--first-fault-secs", type=int)
    parser.add_argument("--recovery-secs", type=int, default=180)
    parser.add_argument("--repair-retry-secs", type=int, default=180)
    parser.add_argument("--max-repair-attempts", type=int, default=2)
    parser.add_argument("--recovery-slo-secs", type=int, default=120)
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
