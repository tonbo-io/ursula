#!/usr/bin/env python3
import json
import subprocess
import tempfile
import threading
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from ursula_chaos_agent import (
    CATCH_UP_RECOVERY_SLO_SECS,
    IMPAIRMENT_SCENARIOS,
    NODE_SERVICE_UNIT,
    WORKLOAD_ROLLOVER_UNKNOWN_GRACE_SECS,
    ChaosAgent,
    Node,
    ProducerState,
    WorkloadStream,
    _downsample_history,
    _prune_history,
    _published_started_at,
)


class ChaosAgentStateTest(unittest.TestCase):
    def test_restore_preserves_status_timeline_and_fault_schedule(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.history = deque()
        agent.events = deque()
        agent.restored_started_at = None
        agent.restored_workload_coverage = {}
        agent.last_fault = None
        agent.next_fault_at = None
        agent.injections = deque(maxlen=32)
        agent.active_injection_id = None
        agent.active_fault = None
        agent.nodes = []
        next_fault = datetime.now(timezone.utc) + timedelta(hours=1)
        previous = {
            "started_at": "2026-07-20T00:00:00Z",
            "history": [{"time": "2026-07-20T01:00:00Z", "status": "operational"}],
            "events": [{"time": "2026-07-20T01:00:00Z", "level": "info"}],
            "workload": {"coverage": {"probes": {"reader": {"covered": True}}}},
            "chaos": {
                "last_fault": "pod_delete on ursula-0",
                "next_fault_after": next_fault.isoformat().replace("+00:00", "Z"),
                "injections": [{"id": 4, "status": "recovered", "recovered_at": "2026-07-20T02:00:00Z"}],
            },
        }
        agent.load_previous_status = lambda: previous

        agent.restore_published_state()

        self.assertEqual(agent.restored_started_at, datetime(2026, 7, 20, tzinfo=timezone.utc))
        self.assertEqual(len(agent.history), 1)
        self.assertEqual(agent.last_fault, "pod_delete on ursula-0")
        self.assertEqual(agent.next_fault_at, next_fault)
        self.assertTrue(agent.restored_workload_coverage["probes"]["reader"]["covered"])
        self.assertEqual(agent.injections[-1]["id"], 4)

    def test_s3_restore_failure_does_not_overwrite_published_history(self) -> None:
        agent = object.__new__(ChaosAgent)
        with tempfile.TemporaryDirectory() as directory:
            agent.status_file = Path(directory) / "status.json"
            agent.status_s3_uri = "s3://status/chaos/status.json"
            agent.aws_timeout_secs = 15
            failure = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="AccessDenied"
            )

            with patch("ursula_chaos_agent.run", return_value=failure):
                with self.assertRaisesRegex(RuntimeError, "unable to restore"):
                    agent.load_previous_status()

    def test_missing_s3_status_is_treated_as_first_run(self) -> None:
        agent = object.__new__(ChaosAgent)
        with tempfile.TemporaryDirectory() as directory:
            agent.status_file = Path(directory) / "status.json"
            agent.status_s3_uri = "s3://status/chaos/status.json"
            agent.aws_timeout_secs = 15
            missing = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="HeadObject operation: (404)"
            )

            with patch("ursula_chaos_agent.run", return_value=missing):
                self.assertIsNone(agent.load_previous_status())

    def test_record_payload_is_valid_captured_at_json(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.append_success = 7
        stream = WorkloadStream("record-stream", content_type="application/json")
        producer = ProducerState("producer-1")

        payload = agent.build_payload(512, "ascii", stream, producer, 3, 42)
        record = json.loads(payload)

        self.assertEqual(record["seq"], 3)
        self.assertEqual(record["producer"], "producer-1")
        self.assertTrue(record["captured_at"].endswith("Z"))
        self.assertTrue(payload.endswith(b"\n"))
        self.assertEqual(len(payload), 512)

    def test_kubernetes_profile_deletes_the_target_pod(self) -> None:
        agent = object.__new__(ChaosAgent)
        target = Node("ursula-0", "ursula-0", "http://ursula-0:4437")
        calls: list[tuple[list[Node], bool]] = []
        agent.stop_instances = lambda targets, wait: calls.append((targets, wait))

        agent.apply_fault_scenario("pod_delete", [target])

        self.assertEqual(calls, [([target], False)])

    def test_published_started_at_uses_earliest_health_history(self) -> None:
        process_started_at = datetime(2026, 6, 6, 6, 25, 26, tzinfo=timezone.utc)
        history = [
            {"time": "2026-06-05T18:00:00Z", "status": "partial_outage"},
            {"time": "not-a-time", "status": "unknown"},
            {"time": "2026-06-06T17:22:51Z", "status": "operational"},
        ]

        self.assertEqual(
            _published_started_at(process_started_at, history),
            datetime(2026, 6, 5, 18, 0, 0, tzinfo=timezone.utc),
        )

    def test_published_started_at_falls_back_to_process_start(self) -> None:
        process_started_at = datetime(2026, 6, 6, 6, 25, 26, tzinfo=timezone.utc)

        self.assertEqual(
            _published_started_at(process_started_at, [{"time": "not-a-time"}]),
            process_started_at,
        )

    def test_published_started_at_preserves_restored_start(self) -> None:
        process_started_at = datetime(2026, 6, 6, 17, 27, 10, tzinfo=timezone.utc)
        restored_started_at = datetime(2026, 6, 5, 18, 0, 0, tzinfo=timezone.utc)

        self.assertEqual(
            _published_started_at(process_started_at, [], restored_started_at),
            restored_started_at,
        )

    def test_history_retention_is_time_based_under_extra_publishes(self) -> None:
        now = datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc)
        start = now - timedelta(days=7, hours=2)
        history: deque[dict[str, object]] = deque()
        sample = start
        while sample <= now:
            history.append({"time": sample.isoformat().replace("+00:00", "Z")})
            sample += timedelta(seconds=13)

        _prune_history(history, int(now.timestamp() * 1000))

        oldest = datetime.fromisoformat(str(history[0]["time"]).replace("Z", "+00:00"))
        self.assertGreater(len(history), 40_560)
        self.assertGreaterEqual(oldest, now - timedelta(days=7, hours=1))
        self.assertLess(oldest, now - timedelta(days=7, hours=1) + timedelta(seconds=13))

    def test_downsample_preserves_restored_hourly_bucket_end(self) -> None:
        now = datetime(2026, 7, 18, 8, 30, tzinfo=timezone.utc)
        restored = [
            {
                "time": "2026-07-12T00:00:00Z",
                "status": "operational",
                "append_success_delta": 100,
            }
        ]

        published = _downsample_history(restored, int(now.timestamp() * 1000))

        self.assertEqual(published[0]["time"], "2026-07-12T00:00:00Z")

    def test_reconciles_unresolved_impairment_injection(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.nodes = [
            Node("ursula-chaos-node-1", "i-1", "http://172.31.80.22:4491"),
            Node("ursula-chaos-node-2", "i-2", "http://172.31.31.150:4491"),
        ]
        recover_at = datetime.now(timezone.utc) + timedelta(minutes=1)
        agent.injections = deque(
            [
                {
                    "id": 7,
                    "scenario": "cluster_netem_delay",
                    "target_nodes": ["ursula-chaos-node-1"],
                    "cleanup": "clear_impairment",
                    "status": "injected",
                    "recover_after": recover_at.isoformat().replace("+00:00", "Z"),
                    "start_requested_at": None,
                    "recovered_at": None,
                }
            ]
        )
        agent.active_fault = None
        agent.active_injection_id = None

        agent.reconcile_active_fault_from_injection(datetime.now(timezone.utc))

        self.assertEqual(agent.active_injection_id, 7)
        self.assertIsNotNone(agent.active_fault)
        self.assertEqual(agent.active_fault["scenario"], "cluster_netem_delay")
        self.assertEqual([node.name for node in agent.active_fault["targets"]], ["ursula-chaos-node-1"])
        self.assertEqual(agent.active_fault["cleanup"], "clear_impairment")
        self.assertEqual(
            agent.active_fault_label(),
            f"cluster_netem_delay on ursula-chaos-node-1 until {recover_at.isoformat().replace('+00:00', 'Z')}",
        )

    def test_process_and_network_scenarios_recover_as_impairments(self) -> None:
        # These recover via faultd /clear (thaw) or systemd auto-restart, not via
        # the EC2 stop/start state machine. If one ever drops out of this set it
        # would wrongly wait for instance_state to flip to "stopped" and wedge.
        for scenario in (
            "process_kill",
            "process_freeze",
            "oneway_partition",
            "netem_reorder",
            "netem_duplicate",
        ):
            self.assertIn(scenario, IMPAIRMENT_SCENARIOS)

    def test_apply_fault_scenario_emits_expected_faultd_payloads(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.nodes = [
            Node("ursula-chaos-node-1", "i-1", "http://172.31.80.22:4491"),
            Node("ursula-chaos-node-2", "i-2", "http://172.31.31.150:4491"),
            Node("ursula-chaos-node-3", "i-3", "http://172.31.47.237:4491"),
        ]
        calls: list[tuple[str, dict]] = []
        agent.apply_node_impairment = lambda node, payload: bool(
            calls.append((node.name, payload))
        ) or True
        agent.mark_current_injection_apply_result = lambda applied: None
        agent.event = lambda level, message: None
        target = agent.nodes[0]

        agent.apply_fault_scenario("process_kill", [target])
        self.assertEqual(
            calls[-1],
            ("ursula-chaos-node-1", {"kind": "process", "action": "kill", "units": [NODE_SERVICE_UNIT]}),
        )

        agent.apply_fault_scenario("process_freeze", [target])
        self.assertEqual(calls[-1][1]["action"], "freeze")

        agent.apply_fault_scenario("oneway_partition", [target])
        payload = calls[-1][1]
        self.assertEqual(payload["kind"], "partition")
        self.assertEqual(payload["direction"], "inbound")
        # peers are the two non-target nodes, never the target itself.
        self.assertEqual(set(payload["peer_hosts"]), {"172.31.31.150", "172.31.47.237"})

        agent.apply_fault_scenario("netem_reorder", [target])
        self.assertEqual(calls[-1][1]["reorder_percent"], 25)
        self.assertEqual(calls[-1][1]["scope"], "cluster")

        agent.apply_fault_scenario("netem_duplicate", [target])
        self.assertEqual(calls[-1][1]["duplicate_percent"], 1)

    def test_overall_status_full_raft_sag_reads_degraded_not_outage(self) -> None:
        # full_raft_nodes sags to 0/3 on every injection while the 2/3 quorum
        # keeps committing writes; that must be degraded, not partial_outage —
        # the systemic false-outage bug that polluted whole hours of history.
        overall = ChaosAgent._overall_status(
            integrity_status="operational",
            running_nodes=3,
            metrics_ok=3,
            fully_healthy=False,  # full_raft < expected during the injection
            has_active_fault=True,
            serving_on_quorum=True,  # writes still progressing on the quorum
            workload_started=True,
        )
        self.assertEqual(overall, "degraded_performance")

    def test_overall_status_stalled_writes_is_partial_outage(self) -> None:
        # Majority up but the data plane is not serving -> a real partial_outage.
        overall = ChaosAgent._overall_status(
            integrity_status="operational",
            running_nodes=3,
            metrics_ok=2,
            fully_healthy=False,
            has_active_fault=True,
            serving_on_quorum=False,  # workload not progressing
            workload_started=True,  # already ramped up, so this is a real stall
        )
        self.assertEqual(overall, "partial_outage")

    def test_overall_status_startup_grace_is_not_outage(self) -> None:
        # Agent restart: workload hasn't begun (append_success == 0) so serving
        # reads false, but the majority is up. Must read operational, not a
        # false partial_outage that would stamp the deploy into the history bar.
        overall = ChaosAgent._overall_status(
            integrity_status="operational",
            running_nodes=3,
            metrics_ok=3,
            fully_healthy=False,
            has_active_fault=False,
            serving_on_quorum=False,
            workload_started=False,
        )
        self.assertEqual(overall, "operational")

    def test_catch_up_scenarios_get_longer_recovery_slo(self) -> None:
        # process_kill recovers via raft-memory catch-up (minutes); reusing the
        # short impairment SLO false-trips slo_missed -> repair_failed (#526).
        agent = object.__new__(ChaosAgent)
        agent.recovery_slo_secs = 120
        self.assertEqual(
            agent.effective_recovery_slo_secs("process_kill"), CATCH_UP_RECOVERY_SLO_SECS
        )
        self.assertEqual(agent.effective_recovery_slo_secs("cluster_netem_delay"), 120)
        self.assertEqual(agent.effective_recovery_slo_secs(None), 120)

    def test_exhausted_repair_keeps_injection_active_until_recovery(self) -> None:
        agent = object.__new__(ChaosAgent)
        now = datetime.now(timezone.utc)
        injection = {
            "id": 29,
            "status": "repairing",
            "start_requested_at": (now - timedelta(minutes=10)).isoformat(),
            "slo_missed_at": (now - timedelta(minutes=5)).isoformat(),
            "recovered_at": None,
            "repair_attempts": 2,
            "timeline": [],
        }
        agent.injections = deque([injection])
        agent.active_injection_id = 29
        agent.active_fault = None
        agent.max_repair_attempts = 2
        agent.next_fault_at = now + timedelta(minutes=1)
        agent.publish_status = lambda: None

        self.assertTrue(agent.repair_unrecovered_injection(now))

        self.assertEqual(injection["status"], "repair_failed")
        self.assertIsNone(agent.next_fault_at)
        self.assertEqual(agent.active_injection_id, 29)
        self.assertIs(agent.current_injection(), injection)

    def test_parse_producer_seq_conflict(self) -> None:
        parsed = ChaosAgent.parse_producer_seq_conflict(
            b"core 1 raft group 3 operation failed: "
            b"ProducerSeqConflict: producer 'chaos-agent-021' expected sequence 2908, received 2938"
        )

        self.assertEqual(parsed, ("chaos-agent-021", 2908, 2938))
        self.assertIsNone(ChaosAgent.parse_producer_seq_conflict(b"ProducerEpochStale"))

    def test_recover_producer_seq_conflict_resyncs_stream_and_bumps_epoch(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.nodes = [
            Node("n1", "i-1", "http://n1:4491"),
            Node("n2", "i-2", "http://n2:4491"),
        ]
        agent.state_lock = threading.Lock()
        agent.events = deque()
        agent.event = lambda level, message: agent.events.append((level, message))
        producer = ProducerState("chaos-agent-001", epoch=3)
        stream = WorkloadStream("run-test-0001", next_offset=99)
        other_stream = WorkloadStream("run-test-0002", next_offset=7)
        agent.streams = [stream, other_stream]
        for workload_stream in agent.streams:
            workload_stream.producer_seqs[producer.producer_id] = 42
            workload_stream.producer_epochs[producer.producer_id] = producer.epoch
            workload_stream.pending_producer_appends[
                f"{producer.producer_id}\0{producer.epoch}\0{42}"
            ] = b"pending"
        calls: list[str] = []

        def request(method, url, **kwargs):
            calls.append(url)
            if url.startswith("http://n1"):
                return 200, b"", {"stream-next-offset": "80"}
            return 200, b"", {"stream-next-offset": "120"}

        agent.request = request

        self.assertTrue(
            agent.recover_producer_seq_conflict(
                stream,
                producer,
                expected_seq=40,
                received_seq=42,
            )
        )

        self.assertEqual(calls, [
            "http://n1:4491/chaos/run-test-0001",
            "http://n2:4491/chaos/run-test-0001",
            "http://n1:4491/chaos/run-test-0002",
            "http://n2:4491/chaos/run-test-0002",
        ])
        self.assertEqual(stream.next_offset, 120)
        self.assertEqual(other_stream.next_offset, 120)
        self.assertEqual(producer.epoch, 4)
        for workload_stream in agent.streams:
            self.assertEqual(workload_stream.producer_seqs[producer.producer_id], 0)
            self.assertEqual(workload_stream.producer_epochs[producer.producer_id], 4)
            self.assertEqual(workload_stream.pending_producer_appends, {})
        self.assertEqual(agent.events[-1][0], "warn")

    def verifier_agent(self, nodes: list[Node]) -> ChaosAgent:
        agent = object.__new__(ChaosAgent)
        agent.nodes = nodes
        agent.state_lock = threading.Lock()
        agent.events = deque()
        agent.event = lambda level, message: agent.events.append((level, message))
        agent.active_fault = None
        agent.active_injection_id = None
        agent.injections = deque()
        agent.append_success = 0
        agent.old_sample_every = 1
        agent.verify_attempts = 0
        agent.verified_offsets = 0
        agent.mismatch_count = 0
        agent.read_availability_errors = 0
        agent.verify_counts = {}
        agent.verify_errors = {}
        agent.last_integrity_error = None
        agent.last_read_availability_error = None
        agent.last_integrity_check = None
        agent.last_read_check = None
        agent.last_read_error_check = None
        agent.last_cold_flush = None
        agent.cold_refresh_cursor = 0
        agent.cold_flush_attempts = 0
        agent.cold_flush_success = 0
        agent.cold_flush_noop = 0
        agent.cold_flush_errors = 0
        agent.fault_backend = "ec2"
        return agent

    def test_verifier_reads_back_bytes_and_separates_corruption_from_availability(self) -> None:
        agent = self.verifier_agent([Node("n1", "i-1", "http://n1:4491")])
        agent.verify_modes = ["latest"]
        stream = WorkloadStream("run-test-0001")
        agent.streams = [stream]
        # A 200 append acknowledged at Stream-Next-Offset 30 holds [20, 30).
        agent.record_payload_sample(stream, 30, b"0123456789", "ascii")
        self.assertEqual(stream.recent_payloads[-1].start_offset, 20)
        json_stream = WorkloadStream("run-test-record-0000", content_type="application/json")
        agent.record_payload_sample(json_stream, 4, b"{}\n\n", "ascii")
        self.assertEqual(len(json_stream.recent_payloads), 0)

        stored = {"bytes": b"0123456789"}
        urls: list[str] = []

        def request(method, url, **kwargs):
            urls.append(url)
            query = dict(part.split("=") for part in url.split("?", 1)[1].split("&"))
            start = int(query["offset"]) - 20
            # Serve at most 6 bytes per read, so the verifier follows a short read.
            return 200, stored["bytes"][start : start + min(6, int(query["max_bytes"]))], {}

        agent.request = request
        agent.verify_integrity()
        self.assertEqual(agent.verified_offsets, 1)
        self.assertEqual(agent.verify_counts, {"latest": 1})
        self.assertEqual(urls, [
            "http://n1:4491/chaos/run-test-0001?offset=20&max_bytes=10",
            "http://n1:4491/chaos/run-test-0001?offset=26&max_bytes=4",
        ])

        # A server error serves no bytes: availability, not corruption.
        agent.request = lambda method, url, **kwargs: (500, b"boom", {})
        agent.verify_integrity()
        self.assertEqual(agent.mismatch_count, 0)
        self.assertEqual(agent.read_availability_errors, 1)
        self.assertEqual(agent.verify_errors, {"latest_unavailable": 1})
        self.assertIsNone(agent.last_integrity_error)

        stored["bytes"] = b"0123456X89"
        agent.request = request
        agent.verify_integrity()
        self.assertEqual(agent.mismatch_count, 1)
        self.assertEqual(agent.verify_errors, {"latest_unavailable": 1, "latest": 1})
        self.assertIn("body_prefix=", agent.last_integrity_error)

    def test_verifier_counts_lost_acknowledged_bytes_and_replica_disagreement(self) -> None:
        agent = self.verifier_agent([Node(f"n{i}", f"i-{i}", f"http://n{i}:4491") for i in (1, 2, 3)])
        agent.verify_modes = ["latest"]
        stream = WorkloadStream("run-test-0001")
        agent.streams = [stream]
        agent.record_payload_sample(stream, 30, b"0123456789", "ascii")
        served = {"tail": 26, "n1": b"0123456789"}

        def request(method, url, **kwargs):
            if method == "HEAD":
                return 200, b"", {"stream-next-offset": f"{served['tail']:020d}"}
            node = url.split("//", 1)[1].split(":", 1)[0]
            query = dict(part.split("=") for part in url.split("?", 1)[1].split("&"))
            offset, max_bytes = int(query["offset"]), int(query["max_bytes"])
            if offset > served["tail"]:
                return 416, b"", {"stream-next-offset": str(served["tail"])}
            stored = served.get(node, b"0123456789")[: served["tail"] - 20]
            return 200, stored[offset - 20 : offset - 20 + max_bytes], {}

        agent.request = request
        # Every replica ends at 26 and the leader's tail agrees: the stream
        # lost bytes [26, 30) it acknowledged.
        agent.verify_integrity()
        self.assertEqual(agent.mismatch_count, 1)
        self.assertEqual(agent.read_availability_errors, 0)
        self.assertIn("leader tail 26 < acknowledged 30", agent.last_integrity_error)

        # Replicas served short, but the leader's tail covers the sample:
        # the range was unavailable, not lost.
        served["tail"] = 30
        agent.request = lambda method, url, **kwargs: (
            request(method, url) if method == "HEAD" else (503, b"", {})
        )
        agent.verify_integrity()
        self.assertEqual(agent.mismatch_count, 1)
        self.assertEqual(agent.read_availability_errors, 1)

        # One replica served different bytes and a later one matched: still
        # corruption.
        agent.request = request
        agent.verify_attempts = 0  # incremented to 1 before the read: start at n2
        served["n2"] = b"0123456X89"
        agent.verify_integrity()
        self.assertEqual(agent.mismatch_count, 2)
        self.assertIn("n2 read status=200 body_prefix=", agent.last_integrity_error)

    def test_cold_mode_confirms_samples_below_cold_hot_start_offset(self) -> None:
        agent = self.verifier_agent([Node("n1", "i-1", "http://n1:4491")])
        agent.verify_modes = ["cold"]
        stream = WorkloadStream("run-test-0001")
        agent.streams = [stream]
        agent.record_payload_sample(stream, 4, b"cold", "ascii")
        agent.record_payload_sample(stream, 8, b"warm", "ascii")
        stored = b"coldwarm"

        def request(method, url, **kwargs):
            if method == "HEAD":
                return 200, b"", {"stream-cold-hot-start-offset": "00000000000000000004"}
            query = dict(part.split("=") for part in url.split("?", 1)[1].split("&"))
            start = int(query["offset"])
            return 200, stored[start : start + int(query["max_bytes"])], {}

        agent.request = request
        agent.verify_integrity()

        self.assertEqual([sample.cold_confirmed for sample in stream.recent_payloads], [True, False])
        self.assertEqual(agent.verify_counts, {"cold": 1})
        self.assertEqual(agent.last_read_check["offset"], 0)
        self.assertEqual(agent.cold_flush_attempts, 0)

    def test_cold_flush_that_leaves_sample_hot_is_unavailable_not_an_error(self) -> None:
        agent = self.verifier_agent([Node(f"n{i}", f"i-{i}", f"http://n{i}:4491") for i in (1, 2, 3)])
        stream = WorkloadStream("run-test-0001")
        agent.streams = [stream]
        agent.record_payload_sample(stream, 100, b"0123456789", "ascii")
        sample = stream.recent_payloads[-1]
        posts: list[str] = []

        def request(method, url, **kwargs):
            if method == "HEAD":
                return 200, b"", {"stream-cold-hot-start-offset": "0"}
            posts.append(url)
            return 200, b'{"hot_start_offset": 50}', {}

        agent.request = request
        self.assertFalse(agent.ensure_cold_sample(sample))
        self.assertEqual(posts, ["http://n1:4438/__ursula/flush-cold/chaos/run-test-0001?min_hot_bytes=1&max_bytes=100"])
        self.assertEqual((agent.cold_flush_success, agent.cold_flush_errors), (1, 0))

        # The chart's admin plane is loopback-only on Kubernetes: no POST.
        agent.fault_backend = "kubernetes"
        posts.clear()
        self.assertFalse(agent.ensure_cold_sample(sample))
        self.assertEqual(posts, [])
        self.assertEqual(agent.cold_flush_attempts, 1)

    def test_workload_rollover_forces_progress_after_unknown_append_grace(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.state_lock = threading.Lock()
        agent.next_workload_rollover_at = 100.0
        agent.workload_run_secs = 3600
        agent.rollover_in_progress = False
        agent.rollover_forced_with_unknown_appends = False
        agent.active_append_count = 1
        agent.active_fault = None
        agent.active_injection_id = None
        agent.injections = deque()
        agent.base_run_id = "run-test"
        agent.run_generation = 4
        agent.run_id = "run-test-r0004"
        agent.current_run_started_at = datetime.now(timezone.utc)
        agent.workload_stream_count = 2
        agent.record_stream_count = 1
        old_stream = WorkloadStream("run-test-r0004-record-0000")
        old_stream.pending_producer_appends["producer\0epoch\0seq"] = b"unknown"
        agent.streams = [old_stream]
        agent.producer_probe_stream = WorkloadStream("run-test-r0004-producer-probe")
        agent.producer_probe_epoch = 3
        agent.append_workers = 2
        agent.lane_attempts = [9, 10]
        agent.lane_unresolved_appends = [True, False]
        agent.global_unresolved_append = False
        events: list[tuple[str, str]] = []
        created: list[str] = []
        unregistered: list[str] = []
        agent.event = lambda level, message: events.append((level, message))
        agent.create_workload_run_streams = (
            lambda run_id, streams, probe: created.append(run_id)
        )
        agent.unregister_index_stream = lambda stream: unregistered.append(stream.name)

        with patch(
            "ursula_chaos_agent.time.monotonic",
            return_value=100.0 + WORKLOAD_ROLLOVER_UNKNOWN_GRACE_SECS - 1,
        ):
            agent.maybe_rollover_workload_streams()

        self.assertFalse(agent.rollover_in_progress)
        self.assertEqual(agent.run_generation, 4)
        self.assertEqual(created, [])

        with patch(
            "ursula_chaos_agent.time.monotonic",
            return_value=100.0 + WORKLOAD_ROLLOVER_UNKNOWN_GRACE_SECS + 1,
        ):
            agent.maybe_rollover_workload_streams()

        self.assertTrue(agent.rollover_in_progress)
        self.assertEqual(agent.run_generation, 4)
        self.assertEqual(created, [])

        agent.active_append_count = 0
        with patch(
            "ursula_chaos_agent.time.monotonic",
            return_value=100.0 + WORKLOAD_ROLLOVER_UNKNOWN_GRACE_SECS + 1,
        ):
            agent.maybe_rollover_workload_streams()

        self.assertFalse(agent.rollover_in_progress)
        self.assertEqual(agent.run_generation, 5)
        self.assertEqual(agent.run_id, "run-test-r0005")
        self.assertEqual(created, ["run-test-r0005"])
        self.assertEqual(unregistered, ["run-test-r0004-record-0000"])
        self.assertEqual(agent.lane_unresolved_appends, [False, False])
        self.assertFalse(agent.global_unresolved_append)
        self.assertTrue(any("forcing workload rollover" in message for _, message in events))

    def test_append_count_is_released_when_an_append_raises(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.state_lock = threading.Lock()
        agent.rollover_in_progress = False
        agent.active_append_count = 0

        def fail(_lane_id):
            raise RuntimeError("append failed")

        agent._append_once_active = fail

        with self.assertRaisesRegex(RuntimeError, "append failed"):
            agent.append_once(3)

        self.assertEqual(agent.active_append_count, 0)


if __name__ == "__main__":
    unittest.main()
