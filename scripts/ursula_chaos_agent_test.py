#!/usr/bin/env python3
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone

from ursula_chaos_agent import ChaosAgent, Node


class ChaosAgentStateTest(unittest.TestCase):
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
