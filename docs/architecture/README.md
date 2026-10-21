# Architecture

These documents describe how Ursula is built and why, as the code stands. They are for people
who change the code. The user-facing overview is the
[architecture page](https://ursula.tonbo.io/docs/architecture/overview), and the wire protocol is
in the [specifications](../web/src/content/docs/pages/specs/).

| Document | Covers |
| --- | --- |
| [thread-per-core-multiraft.md](thread-per-core-multiraft.md) | Stream ownership, group placement, the owner-core runtime, Raft integration and transport. |
| [raft-wal.md](raft-wal.md) | The per-core Raft journal: format, fsync policy, durability contract, recovery gate, reclaim, metrics. |
| [bounded-stream-state.md](bounded-stream-state.md) | Why per-stream state stays bounded on a long-lived stream, mechanism by mechanism, and the CI ratchet that holds it. |
| [cluster-membership.md](cluster-membership.md) | Static membership, replica recovery, the admin surface and the Kubernetes integration. |
| [deterministic-simulation-testing.md](deterministic-simulation-testing.md) | The madsim harness, its scenarios, faults, invariants, corpus and audits. |
| [immutable-reads.md](immutable-reads.md) | The chunk and snapshot resources a shared cache can serve: routes, the grid, marking, follower forwarding, validators. |
| [sqlite-vfs.md](sqlite-vfs.md) | The replicating SQLite VFS: the local commit log, ordered replication and the output gate, claims and reconcile, snapshots, local generations, history changes, the API, the tests behind each guarantee, limits. |

A document here states what is true of the code. Plans, open questions and work in progress
belong in GitHub issues and pull requests, and history belongs in git.
