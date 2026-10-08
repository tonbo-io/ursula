# Raft component boundaries

The data-group engine owns recovery tasks and exposes the runtime's existing
`GroupEngine` interface. Construction supplies `RaftGroupEngineOptions`; it does
not open journal writers or individually wire recovery drivers.

| Component | Responsibility and public seam |
| --- | --- |
| `RaftWal` | Node run state, per-core writer lifetime, group log stores and shutdown. The seven primary WAL exports live in `ursula_raft::wal`; persisted-format inspection types live under `wal::diagnostics`. |
| `RecoveryGate::attach` | Bind the follower gate, publish the group's resources, start bootstrap/heal/barrier drivers and retain their task handles. Engine shutdown aborts and joins them; drop cancels startup leftovers. Native gRPC and simulation inject `RecoveryTransport` into the same wiring. |
| `ElectionPolicy` | Combine node shedding and per-group recovery eligibility, refresh OpenRaft's election switch, and validate outbound handoff targets. Transfers use campaign eligibility because receiving a transfer starts an election. Cold-health pressure alone remains electable. |
| `GroupEntry` | Keep the active Raft handle, read barrier, recovery gate and cold-index cache together. Engine factories and fixtures publish them together through the engine. A recovery gate is bound once to its engine and cannot be removed by re-publication. Each RPC retains one entry snapshot through dispatch. The registry routes requests to these entries. |
| `ursula-proto` | Own shared admin requests, responses and recovery status. The complete WAL metric set, including its sample types, remains in runtime. The full metrics response has not yet been unified. |

All server leadership handoffs enter through the registry. A target must be
another voter and must not be a follower this leader knows has reverted its log.
The destination independently rejects a transfer while it cannot campaign.
These checks do not predict an undetected remote disk loss; the receiver's gate
and Raft protocol remain necessary.

Bootstrap and heal wait on server-state and gate notifications; the barrier
driver also watches apply progress. They retain deadlines for
remote retries and stall reporting. Repeating the same probe or repair step is
rate limited even if that operation itself publishes metrics. The recovery
tasks do not retain the registry; attachment still publishes group resources. If another voter loses its log during a
joint membership change, repair restores replication before trying to finish
that change. WAL segment pressure reaches the snapshot
driver directly through the WAL's lagging-group handle.

The fault-injection `InProcess*` network is compiled only under `cfg(test)` or
`cfg(madsim)`, not into the production library.

## Admin quorum proof

`GET /__ursula/raft/{group}/quorum` requires the process incarnation header. It
confirms a fresh registered ReadIndex round and checks that the committed leader
vote did not change across the round. The typed response identifies the group,
leader, term and required applied index. It is an observation, not permission to
remove a voter. It remains readable after a maintenance fence is retired.

`ursulactl` uses only the admin protocol and has no Raft RPC compatibility
adapter or `ursula-raft` dependency. Quorum proofs and self-election requests
require process identity. Low-level Rust WAL exports and engine constructors
have changed; callers use `RaftWal` and the engine options above.
