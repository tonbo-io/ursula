#!/usr/bin/env python3
import json
import random
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
    PLACEMENT_SCENARIOS,
    WORKLOAD_ROLLOVER_UNKNOWN_GRACE_SECS,
    ChaosAgent,
    Node,
    ProducerState,
    WorkloadStream,
    _downsample_history,
    _prune_history,
    _published_started_at,
    active_nodes,
    cluster_retirements,
    freshest_placement,
    group_targets,
    node_matches_placement,
    placement_refusal,
    plan_moves,
)
from ursula_chaos_faultd import wal_mount_check


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
            Node("ursula-chaos-node-1", "i-1", "http://192.0.2.1:4491"),
            Node("ursula-chaos-node-2", "i-2", "http://192.0.2.2:4491"),
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
            Node("ursula-chaos-node-1", "i-1", "http://192.0.2.1:4491"),
            Node("ursula-chaos-node-2", "i-2", "http://192.0.2.2:4491"),
            Node("ursula-chaos-node-3", "i-3", "http://192.0.2.3:4491"),
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
        self.assertEqual(set(payload["peer_hosts"]), {"192.0.2.2", "192.0.2.3"})

        agent.apply_fault_scenario("netem_reorder", [target])
        self.assertEqual(calls[-1][1]["reorder_percent"], 25)
        self.assertEqual(calls[-1][1]["scope"], "cluster")

        agent.apply_fault_scenario("netem_duplicate", [target])
        self.assertEqual(calls[-1][1]["duplicate_percent"], 1)

        # The cluster-plane faults take their peers from the node hosts, so a
        # cluster of any size and addressing is covered: a netem scoped to
        # each target's peers, a partition from every node outside the
        # targets. The bug this catches: a fixed address list, which covers
        # only the hosts it names.
        calls.clear()
        agent.apply_fault_scenario("cluster_netem_delay", agent.nodes)
        self.assertEqual(
            {name: payload["cluster_subnets"] for name, payload in calls},
            {
                "ursula-chaos-node-1": ["192.0.2.2/32", "192.0.2.3/32"],
                "ursula-chaos-node-2": ["192.0.2.1/32", "192.0.2.3/32"],
                "ursula-chaos-node-3": ["192.0.2.1/32", "192.0.2.2/32"],
            },
        )
        self.assertEqual((calls[0][1]["delay_ms"], calls[0][1]["jitter_ms"]), (250, 75))
        for scenario in ("cluster_partition", "asymmetric_partition"):
            calls.clear()
            agent.apply_fault_scenario(scenario, [target])
            self.assertEqual(calls, [("ursula-chaos-node-1", {"kind": "partition", "peer_hosts": ["192.0.2.2", "192.0.2.3"]})])

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
        # process_kill recovers via WAL replay and peer catch-up (minutes); reusing
        # the short impairment SLO false-trips slo_missed -> repair_failed (#526).
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
        # A real 0.8 answer: the code in the error body, the seqs in the
        # typed producer headers.
        body = (
            b'{"error":{"code":"PRODUCER_SEQ_CONFLICT",'
            b'"message":"producer \'chaos-agent-021\' expected sequence 2908, received 2938"}}'
        )
        headers = {
            "producer-epoch": "3",
            "producer-expected-seq": "2908",
            "producer-received-seq": "2938",
        }
        self.assertEqual(
            ChaosAgent.parse_producer_seq_conflict(409, body, headers), (2908, 2938)
        )
        self.assertIsNone(
            ChaosAgent.parse_producer_seq_conflict(
                403,
                b'{"error":{"code":"PRODUCER_EPOCH_STALE","message":"stale"}}',
                {"producer-epoch": "4"},
            )
        )
        # Another 409, such as a content-type mismatch, is no seq gap.
        self.assertIsNone(
            ChaosAgent.parse_producer_seq_conflict(
                409,
                b'{"error":{"code":"CONTENT_TYPE_MISMATCH","message":"m"}}',
                {},
            )
        )
        # 0.7's plain text is no longer read.
        self.assertIsNone(
            ChaosAgent.parse_producer_seq_conflict(
                409,
                b"ProducerSeqConflict: producer 'p' expected sequence 1, received 2",
                headers,
            )
        )

    def test_transient_append_failures_are_read_from_codes(self) -> None:
        no_leader = b'{"error":{"code":"NO_LEADER","message":"no leader of the stream\'s group is known; retry"}}'
        self.assertTrue(ChaosAgent.is_transient_append_failure(503, no_leader, {}))
        self.assertTrue(
            ChaosAgent.is_transient_append_failure(503, b"{}", {"retry-after": "1"})
        )
        self.assertFalse(
            ChaosAgent.is_transient_append_failure(
                503, b'{"error":{"code":"INTERNAL","message":"the server failed"}}', {}
            )
        )
        self.assertEqual(
            ChaosAgent.error_code(
                b'{"error":{"code":"COLD_BACKPRESSURE","message":"the stream\'s group waits for the cold tier; retry"}}'
            ),
            "COLD_BACKPRESSURE",
        )
        self.assertIsNone(ChaosAgent.error_code(b"ColdBackpressure: plain text"))

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
        agent.node_ids = {}
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
            if method == "GET":
                return 200, b'{"raft_groups":[],"process_node_id":1,"process_incarnation":"00000000000000000000000000000001"}', {}
            self.assertEqual(kwargs["headers"], {"x-ursula-process-incarnation":"00000000000000000000000000000001"})
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
        agent.event = lambda level, message: events.append((level, message))
        agent.create_workload_run_streams = (
            lambda run_id, streams, probe: created.append(run_id)
        )

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



def placement_cluster(groups, *, nodes=(1, 2, 3, 4, 5), retirements=None, lifecycles=None):
    """A bootstrapped `Cluster` as `GET /__ursula/placement` encodes it."""
    lifecycles = lifecycles or {}
    encoded = {}
    for group, (state, *voters) in groups.items():
        since = {"term": 2, "leader": 1, "index": 10 + group}
        if state == "seeded":
            encoded[str(group)] = {"seeded": {"since": since, "voters": voters[0], "initializer": voters[0][0]}}
        elif state == "stable":
            encoded[str(group)] = {
                "live": {"since": since, "converged_at": since, "target": {"stable": {"voters": voters[0]}}}
            }
        else:
            encoded[str(group)] = {
                "live": {
                    "since": since,
                    "converged_at": since,
                    "target": {"moving": {"from": voters[0], "to": voters[1], "abandoned": [], "since_ms": 1}},
                }
            }
    return {
        "params": {"group_count": len(groups), "replication_factor": 3, "meta_voters": [1, 2, 3],
                   "max_concurrent_moves": 4},
        "nodes": {str(node): {"lifecycle": lifecycles.get(node, "active"), "since_ms": 1, "root": None}
                  for node in nodes},
        "groups": encoded,
        "retirements": {
            str(group): {str(node): {"created_at": {"term": 2, "leader": 1, "index": 99}} for node in owed}
            for group, owed in (retirements or {}).items()
        },
    }


def replica(group, voters, *, learners=(), leader=1, term=3):
    return {
        "raft_group_id": group,
        "current_leader": leader,
        "voter_ids": list(voters),
        "learner_ids": list(learners),
        "current_term": term,
        "committed_index": 5,
        "last_applied_index": 5,
    }


class PlacementTest(unittest.TestCase):
    def test_a_node_is_complete_when_it_hosts_exactly_what_its_placement_gives_it(self) -> None:
        # The bug this catches: judging every node against "every node votes
        # in every group", which reads every node of a placed cluster as
        # incomplete, or counting a replica its placement dropped as settled.
        targets = group_targets(
            placement_cluster(
                {0: ("stable", [1, 2, 3]), 1: ("moving", [1, 2, 3], [2, 3, 4]), 2: ("seeded", [2, 3, 4])}
            )
        )
        node = lambda node_id, *states: {  # noqa: E731
            "node_id": node_id, "metrics_state": "ok", "raft_group_states": list(states)
        }
        self.assertTrue(
            node_matches_placement(node(1, replica(0, [1, 2, 3]), replica(1, [1, 2, 3], learners=[4])), targets)
        )
        self.assertTrue(node_matches_placement(node(4, replica(1, [1, 2, 3], learners=[4]), replica(2, [2, 3, 4])), targets))
        self.assertTrue(node_matches_placement(node(5), targets))
        # A node missing a placed replica, a replica its placement dropped, a
        # settled group with other voters or a learner, and a replica with no
        # leader are not complete.
        self.assertFalse(node_matches_placement(node(1, replica(0, [1, 2, 3])), targets))
        self.assertFalse(node_matches_placement(node(5, replica(0, [1, 2, 3])), targets))
        self.assertFalse(
            node_matches_placement(node(4, replica(1, [1, 2, 3]), replica(2, [2, 3, 4], learners=[5])), targets)
        )
        self.assertFalse(
            node_matches_placement(node(4, replica(1, [1, 2, 3]), replica(2, [1, 2, 3, 4])), targets)
        )
        self.assertFalse(
            node_matches_placement(node(4, replica(1, [1, 2, 3], leader=None), replica(2, [2, 3, 4])), targets)
        )

    def test_the_freshest_view_is_the_one_that_applied_most(self) -> None:
        older = {"view": {"bootstrapped": {"tag": "older"}}, "applied": {"term": 3, "leader": 2, "index": 90}}
        newer = {"view": {"bootstrapped": {"tag": "newer"}}, "applied": {"term": 4, "leader": 1, "index": 7}}
        forming = {"view": "meta_catching_up", "applied": None}
        self.assertIs(freshest_placement([older, forming, newer]), newer)
        self.assertIsNone(freshest_placement([forming]))

    def test_planned_moves_keep_the_factor_and_never_place_a_retired_replica_again(self) -> None:
        cluster = placement_cluster(
            {
                0: ("stable", [1, 2, 3]),
                1: ("stable", [1, 2, 3]),
                2: ("stable", [2, 3, 4]),
                3: ("moving", [1, 2, 3], [1, 2, 5]),
                4: ("seeded", [3, 4, 5]),
            },
            retirements={0: [4], 1: [4]},
            lifecycles={5: "cordoned"},
        )
        targets = group_targets(cluster)
        active = active_nodes(cluster)
        self.assertEqual(active, [1, 2, 3, 4])
        plan = plan_moves(targets, active, cluster_retirements(cluster), 10, random.Random(7))
        # Groups 0 and 1 would only move to node 4, whose retirement there is
        # pending, and node 5 takes no new replica: only group 2 moves.
        self.assertEqual([move["group"] for move in plan], [2])
        (move,) = plan
        self.assertEqual(len(move["to"]), 3)
        self.assertNotIn(move["incoming"], move["from"])
        self.assertEqual(set(move["to"]), set(move["from"]) - {move["outgoing"]} | {move["incoming"]})
        self.assertEqual(move["incoming"], 1)
        # A fill names its incoming node, and a count bounds the plan.
        fill = plan_moves(
            group_targets(placement_cluster({group: ("stable", [1, 2, 3]) for group in range(6)})),
            [1, 2, 3, 4, 5],
            {},
            4,
            random.Random(3),
            incoming=5,
        )
        self.assertEqual(len(fill), 4)
        self.assertTrue(all(move["incoming"] == 5 for move in fill))

    def test_a_refusal_reads_the_kernels_own_variant(self) -> None:
        refused = {"reason": {"refused": {"move_limit": {"limit": 4, "moving": 4}}}, "write_effect": "not_applied"}
        self.assertEqual(placement_refusal(json.dumps(refused).encode()), "move_limit")
        unit = {"reason": {"refused": "not_bootstrapped"}, "write_effect": "not_applied"}
        self.assertEqual(placement_refusal(json.dumps(unit).encode()), "not_bootstrapped")
        self.assertEqual(placement_refusal(json.dumps({"reason": "no_leader"}).encode()), "no_leader")
        self.assertIsNone(placement_refusal(b"Not Found"))

    def test_each_placement_scenario_plans_its_change_and_its_companion_on_the_named_role(self) -> None:
        # Groups 0 to 2 vote on [1, 2, 3] and each is led by node 2. Meta
        # votes on [2, 3, 5] and node 5 leads it. A third of the groups is one
        # move, of a random group: its voter with the most replicas (node 1,
        # the first of a tie) leaves and the active node with the fewest
        # (node 4, the first of a tie) joins. So the source is 1, the target
        # 4, the data leader 2, the meta leader 5 and the voter that stays
        # and does not lead 3: every role is another node.
        moves = {group: ("stable", [1, 2, 3]) for group in range(3)}
        filled = {**moves, 3: ("stable", [2, 3, 4])}
        rows = [
            # (scenario, backend, groups, lifecycles, expected targets, expected fault, bug caught)
            ("move_third", "ec2", moves, {}, [4], None, "no companion targets the change's incoming nodes"),
            ("move_netem", "ec2", moves, {}, [1, 2, 3, 4, 5], "cluster_netem_delay",
             "netem on one node, or a second netem path beside cluster_netem_delay"),
            ("move_kill_source", "ec2", moves, {}, [1], "process_kill", "the source taken from the incoming side"),
            ("move_kill_source", "kubernetes", moves, {}, [1], "pod_force_delete",
             "a kill on Kubernetes that needs faultd"),
            ("move_kill_target", "ec2", moves, {}, [4], "process_kill", "the target taken from the outgoing side"),
            ("move_kill_target", "kubernetes", moves, {}, [4], "pod_force_delete", "kill not mapped per backend"),
            ("move_kill_data_leader", "ec2", moves, {}, [2], "process_kill",
             "the data leader read as the meta leader or a mover"),
            ("move_kill_meta_leader", "kubernetes", moves, {}, [5], "pod_force_delete",
             "the meta leader read as a data group's leader"),
            ("move_power_cycle", "ec2", moves, {}, [4], "power_cycle", "the power cycle off the target"),
            ("move_slow_disk", "ec2", moves, {}, [4], "slow_disk", "the slow disk off the target"),
            ("move_blackhole_voter", "ec2", moves, {}, [3], "asymmetric_partition",
             "the blackholed staying voter is the group's leader, or a second partition path"),
            # Node 4 holds a replica, so node 5 holds the fewest.
            ("move_fill_node", "ec2", filled, {}, [5], None, "the fill lands on a node that is not the emptiest"),
            # Nodes 1, 2 and 3 hold the most replicas, and 2 and 3 vote in
            # meta: 1 leaves.
            ("decommission", "ec2", moves, {}, [1], None, "a meta voter decommissioned"),
            # Without node 4 three nodes stay active, and one leaving drops
            # below the replication factor of 3.
            ("decommission", "ec2", moves, {4: "cordoned", 5: "cordoned"}, None, None,
             "a decommission that drops below the replication factor"),
        ]
        self.assertEqual({row[0] for row in rows}, set(PLACEMENT_SCENARIOS))
        for scenario, backend, groups, lifecycles, expected, fault, bug in rows:
            with self.subTest(scenario=scenario, backend=backend, bug=bug):
                agent = self.placement_agent()
                agent.nodes = [Node(f"ursula-{node}", f"i-{node}", f"http://192.0.2.{node}:4437") for node in range(1, 6)]
                agent.node_ids = {node.name: index for index, node in enumerate(agent.nodes, start=1)}
                agent.fault_backend = backend
                agent.placement_change = None
                cluster = placement_cluster(groups, lifecycles=lifecycles)
                cluster["params"]["meta_voters"] = [2, 3, 5]
                status = {"view": {"bootstrapped": cluster}, "applied": {"term": 2, "leader": 5, "index": 50},
                          "meta": {"running": {"leader": 5}}}
                agent.confirmed_cluster = lambda cluster=cluster: (agent.nodes[0], cluster)

                def sample_node(node, groups=groups, status=status):
                    node_id = agent.node_ids[node.name]
                    return {
                        "name": node.name, "node_id": node_id, "metrics_state": "ok", "placement": status,
                        "raft_group_states": [
                            {**replica(group, voters, leader=2), "node_id": node_id}
                            for group, (_state, voters) in groups.items()
                            if node_id in voters
                        ],
                    }

                agent.sample_node = sample_node
                started = agent.start_placement_change(scenario, 1)
                if expected is None:
                    self.assertIsInstance(started, str)
                    self.assertIsNone(agent.placement_change)
                    continue
                targets, companion = started
                self.assertEqual(sorted(agent.node_ids[node.name] for node in targets), expected)
                self.assertEqual(companion, fault)
                change = agent.placement_change
                if scenario == "decommission":
                    self.assertEqual(change["decommission"]["node"], 1)
                    self.assertEqual(change["queue"], [])
                    continue
                for move in change["queue"]:
                    self.assertEqual(len(move["to"]), 3)
                if scenario == "move_fill_node":
                    self.assertTrue(change["queue"])
                    self.assertTrue(all(move["incoming"] == 5 for move in change["queue"]))
                else:
                    (move,) = change["queue"]
                    self.assertEqual((move["from"], move["to"]), ([1, 2, 3], [2, 3, 4]))

    def placement_agent(self) -> ChaosAgent:
        agent = object.__new__(ChaosAgent)
        agent.nodes = [Node("ursula-0", "ursula-0", "http://ursula-0:4437")]
        agent.events = deque()
        agent.state_lock = threading.Lock()
        agent.append_latencies = deque()
        agent.last_group_terms = {0: 3, 1: 3, 2: 3}
        agent.injections = deque([{"id": 1, "placement": {"action": "move_third"}}])
        agent.event = lambda level, message: agent.events.append((level, message))
        return agent

    def test_moves_requeue_on_the_move_limit_and_a_lost_answer_resolves_from_the_view(self) -> None:
        agent = self.placement_agent()
        moves = [
            {"group": group, "from": [1, 2, 3], "to": [2, 3, 4], "outgoing": 1, "incoming": 4} for group in range(3)
        ]
        agent.placement_change = {
            "injection_id": 1, "action": "move_third", "started_at": 0.0, "queue": list(moves),
            "moves": {}, "refused": {}, "decommission": None,
        }
        view = placement_cluster({group: ("stable", [1, 2, 3]) for group in range(3)})
        agent.confirmed_cluster = lambda: (agent.nodes[0], view)
        answers = [
            (200, b'{"since": {"term": 2, "leader": 1, "index": 40}}', {}),
            (409, json.dumps({"reason": {"refused": {"move_limit": {"limit": 1, "moving": 1}}}}).encode(), {}),
        ]
        submitted: list[tuple[str, dict]] = []

        def admin_request(node, method, path, *, json_body=None):
            submitted.append((path, json_body))
            return answers.pop(0)

        agent.admin_request = admin_request
        agent.drive_placement_change()
        self.assertEqual(list(agent.placement_change["moves"]), [0])
        self.assertEqual([move["group"] for move in agent.placement_change["queue"]], [1, 2])
        self.assertEqual(submitted[0][1]["expected"], {"term": 2, "leader": 1, "index": 10})
        # The second submit's answer was lost: the view shows group 1 moving
        # to its target, so it is accepted without a resubmit. Group 2's
        # placement moved on, so the move is refused, never sent stale.
        view = placement_cluster(
            {0: ("moving", [1, 2, 3], [2, 3, 4]), 1: ("moving", [1, 2, 3], [2, 3, 4]), 2: ("stable", [1, 2, 5])}
        )
        agent.drive_placement_change()
        self.assertEqual(len(submitted), 2)
        self.assertEqual(sorted(agent.placement_change["moves"]), [0, 1])
        self.assertEqual(agent.placement_change["queue"], [])
        self.assertEqual(agent.placement_change["refused"], {"placement_changed": 1})

    def test_a_change_settles_once_every_retired_replica_is_torn_down_and_records_its_measures(self) -> None:
        agent = self.placement_agent()
        move = {"group": 0, "from": [1, 2, 3], "to": [2, 3, 4], "outgoing": 1, "incoming": 4,
                "accepted_at": 100.0, "start_term": 3}
        agent.placement_change = {
            "injection_id": 1, "action": "move_third", "started_at": 100.0, "queue": [],
            "moves": {0: move}, "refused": {"stale_placement": 1}, "decommission": None,
        }
        # Group 0 moves, group 1 does not: before the change both write
        # fast, during it the moving group's writes are slow.
        # Group 0's worst append inside its move (accepted at 100, stable at
        # 110) is 80 ms. A slower one after it is stable counts toward the
        # moving p99 over the change, never toward the move's own spike.
        agent.append_latencies.extend(
            [(95.0, 0, 5.0), (96.0, 1, 6.0), (101.0, 0, 80.0), (102.0, 1, 9.0), (103.0, None, 500.0),
             (112.0, 0, 400.0)]
        )
        topology = {"raft_groups": [{"raft_group_id": 0, "replicas": [{"current_term": 5}, {"current_term": 4}]}]}

        def observe(retirements):
            cluster = placement_cluster({0: ("stable", [2, 3, 4]), 1: ("stable", [1, 2, 3])}, retirements=retirements)
            placement = {"stage": "bootstrapped", "cluster": cluster, "targets": group_targets(cluster),
                         "retirements": cluster_retirements(cluster)}
            agent.observe_placement_change(placement, topology)

        with patch("ursula_chaos_agent.time.monotonic", return_value=110.0):
            observe({0: [1]})
        self.assertIsNotNone(agent.placement_change)
        self.assertEqual(move["stable_at"], 110.0)
        with patch("ursula_chaos_agent.time.monotonic", return_value=115.0):
            observe({})
        self.assertIsNone(agent.placement_change)
        measures = agent.injections[0]["placement"]
        self.assertEqual(measures["move_secs_max"], 10.0)
        self.assertEqual(measures["teardown_secs_max"], 5.0)
        self.assertEqual(measures["elections_total"], 2)
        self.assertEqual(measures["write_p99_ms_moving"], 400.0)
        self.assertEqual(measures["write_p99_ms_not_moving"], 9.0)
        self.assertEqual(measures["write_p99_ms_before"], 6.0)
        self.assertEqual(measures["move_write_max_ms"], 80.0)
        self.assertEqual(measures["move_write_max_group"], 0)
        self.assertEqual(measures["moves_refused"], {"stale_placement": 1})

    def test_groups_another_actor_moves_count_as_moving_and_get_their_time(self) -> None:
        # The bug this catches: classifying a group as moving only when the
        # agent's own move of it was accepted. The decommission driver moves
        # every group off its node, and a move recorded as refused after a
        # lost answer may still commit: both would count as not moving, so
        # their joint-phase writes would land in the not-moving p99.
        agent = self.placement_agent()
        agent.placement_change = {
            "injection_id": 1, "action": "decommission", "started_at": 100.0, "queue": [], "moves": {},
            "refused": {"placement_changed": 1}, "observed": {},
            "decommission": {"node": 1, "accepted_at": 100.0, "removed_at": None, "refused": None},
        }
        agent.append_latencies.extend([(105.0, 0, 300.0), (106.0, 1, 9.0), (107.0, 2, 120.0)])
        topology = {"raft_groups": [{"raft_group_id": 0, "replicas": [{"current_term": 4}]}]}

        def observe(at, groups, retirements, lifecycle):
            cluster = placement_cluster(groups, retirements=retirements, lifecycles={1: lifecycle})
            placement = {"stage": "bootstrapped", "cluster": cluster, "targets": group_targets(cluster),
                         "retirements": cluster_retirements(cluster)}
            with patch("ursula_chaos_agent.time.monotonic", return_value=at):
                agent.observe_placement_change(placement, topology)

        # Group 0 is seen moving, then stable with node 1's replica owed,
        # then torn down. Group 2 moved between two samples: first seen
        # stable with its retirement pending, so it counts as moving but has
        # no time. Group 1 never moves.
        observe(102.0, {0: ("moving", [1, 2, 3], [2, 3, 4]), 1: ("stable", [2, 3, 4]), 2: ("stable", [2, 3, 5])},
                {2: [1]}, "decommissioning")
        observe(110.0, {0: ("stable", [2, 3, 4]), 1: ("stable", [2, 3, 4]), 2: ("stable", [2, 3, 5])},
                {0: [1], 2: [1]}, "decommissioning")
        self.assertIsNotNone(agent.placement_change)
        observe(114.0, {0: ("stable", [2, 3, 4]), 1: ("stable", [2, 3, 4]), 2: ("stable", [2, 3, 5])},
                {}, "removed")
        self.assertIsNone(agent.placement_change)
        measures = agent.injections[0]["placement"]
        self.assertEqual(measures["moves_accepted"], 0)
        self.assertEqual(measures["moves_observed"], 2)
        self.assertEqual(measures["write_p99_ms_not_moving"], 9.0)
        self.assertEqual(measures["write_p99_ms_moving"], 300.0)
        self.assertEqual(measures["move_secs_max"], 8.0)
        self.assertEqual(measures["teardown_secs_max"], 4.0)
        self.assertEqual(measures["elections_total"], 1)
        self.assertEqual(measures["move_write_max_ms"], 300.0)
        self.assertEqual(measures["decommission_secs"], 14.0)


class FaultdTest(unittest.TestCase):
    def test_disk_faults_refuse_a_wal_path_on_the_root_filesystem(self) -> None:
        # The bug this catches: `fsfreeze` or the disk limits on a WAL that
        # sits on `/`, which freezes or throttles the host's root filesystem
        # and faultd with it, so the `/clear` that thaws it may never run.
        with self.assertRaises(ValueError):
            wal_mount_check("/")
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
            wal_mount_check(directory)


if __name__ == "__main__":
    unittest.main()
