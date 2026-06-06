#!/usr/bin/env python3
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone

from ursula_chaos_agent import ChaosAgent, Node, _published_started_at


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


if __name__ == "__main__":
    unittest.main()
