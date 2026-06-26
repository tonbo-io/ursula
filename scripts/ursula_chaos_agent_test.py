#!/usr/bin/env python3
import threading
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone

from ursula_chaos_agent import (
    CATCH_UP_RECOVERY_SLO_SECS,
    IMPAIRMENT_SCENARIOS,
    NODE_SERVICE_UNIT,
    ChaosAgent,
    Node,
    ProducerState,
    Setsum,
    WorkloadStream,
    _minimum_history_points,
    _published_started_at,
)


class ChaosAgentStateTest(unittest.TestCase):
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

    def test_minimum_history_points_covers_published_window(self) -> None:
        # Seven days of hourly bars plus the last raw hour at a 15s publish cadence.
        self.assertEqual(_minimum_history_points(15), 40560)
        self.assertGreater(_minimum_history_points(10), _minimum_history_points(15))

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
        server_setsum = Setsum()
        server_setsum.insert_vectored([b"server"])
        zero_setsum = Setsum().hexdigest()
        calls: list[str] = []

        def request(method, url, **kwargs):
            calls.append(url)
            if url.startswith("http://n1"):
                return 200, b"", {
                    "stream-next-offset": "80",
                    "stream-integrity-total-records": "8",
                    "stream-integrity-total-setsum": zero_setsum,
                }
            return 200, b"", {
                "stream-next-offset": "120",
                "stream-integrity-total-records": "12",
                "stream-integrity-total-setsum": server_setsum.hexdigest(),
            }

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
        self.assertEqual(stream.expected_live_setsum.hexdigest(), server_setsum.hexdigest())
        self.assertEqual(other_stream.next_offset, 120)
        self.assertEqual(other_stream.expected_live_setsum.hexdigest(), server_setsum.hexdigest())
        self.assertEqual(producer.epoch, 4)
        for workload_stream in agent.streams:
            self.assertEqual(workload_stream.producer_seqs[producer.producer_id], 0)
            self.assertEqual(workload_stream.producer_epochs[producer.producer_id], 4)
            self.assertEqual(workload_stream.pending_producer_appends, {})
        self.assertEqual(agent.events[-1][0], "warn")

    def test_pending_integrity_resync_blocks_verifier_until_resynced(self) -> None:
        agent = object.__new__(ChaosAgent)
        agent.nodes = [Node("n1", "i-1", "http://n1:4491")]
        agent.state_lock = threading.Lock()
        agent.events = deque()
        agent.event = lambda level, message: agent.events.append((level, message))
        agent.global_unresolved_append = False
        agent.lane_unresolved_appends = []
        stream = WorkloadStream("run-test-0001", next_offset=40)
        stream.needs_integrity_resync = True
        agent.streams = [stream]
        agent.producer_probe_stream = WorkloadStream("run-test-producer-probe")
        server_setsum = Setsum()
        server_setsum.insert_vectored([b"server"])
        responses = [
            (0, b"timeout", {}),
            (
                200,
                b"",
                {
                    "stream-next-offset": "80",
                    "stream-integrity-total-records": "8",
                    "stream-integrity-total-setsum": server_setsum.hexdigest(),
                },
            ),
        ]

        def request(method, url, **kwargs):
            return responses.pop(0)

        agent.request = request

        self.assertTrue(agent.has_unknown_appends_locked())

        agent.retry_pending_integrity_resyncs()

        self.assertTrue(stream.needs_integrity_resync)
        self.assertTrue(agent.has_unknown_appends_locked())

        agent.retry_pending_integrity_resyncs()

        self.assertFalse(stream.needs_integrity_resync)
        self.assertFalse(agent.has_unknown_appends_locked())
        self.assertEqual(stream.next_offset, 80)
        self.assertEqual(stream.expected_live_setsum.hexdigest(), server_setsum.hexdigest())


if __name__ == "__main__":
    unittest.main()
