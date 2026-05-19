# Runtime Ecosystem Evaluation

## Summary

Thread-per-core is a strong architectural fit for a sharded multi-Raft durable stream runtime, but monoio should not be adopted as the default HTTP/runtime stack until ecosystem gaps are resolved.

The near-term recommendation is:

1. Build the durable stream core around explicit shard ownership and message passing.
2. Keep Tokio/axum/tonic for the first migration milestone.
3. Add a monoio experiment only for shard actors and OpenRaft runtime integration.
4. Revisit monoio for HTTP ingress after replacing or bypassing axum/tonic dependencies.

## Evidence

- `openraft-rt-monoio` provides `MonoioRuntime` for OpenRaft, but requires OpenRaft default features to be disabled and `single-threaded` to be enabled. With `single-threaded`, the `Raft` handle is no longer `Send` or `Sync`.
- `openraft-rt-monoio` still depends on some Tokio primitives for `Watch` and `Mutex` because monoio/local-sync do not provide equivalents.
- monoio is explicitly designed as a thread-per-core runtime with io_uring/epoll/kqueue, and its documentation notes compatibility issues from its custom I/O abstraction.
- monoio documentation also warns that unbalanced workloads can perform worse than Tokio because cores may not be fully utilized.
- axum documents that it is designed to work with Tokio and hyper; runtime and transport independence is not a current goal.
- hyper 1.x has runtime traits, but using that directly would mean dropping below axum's `axum::serve` convenience layer and building custom integration.

Primary sources:

- https://docs.rs/openraft-rt-monoio/latest/openraft_rt_monoio/
- https://docs.rs/monoio/latest/monoio/
- https://docs.rs/axum/latest/axum/
- https://hyper.rs/guides/1/init/runtime/

## Decision

Monoio is feasible for a shard-local OpenRaft actor model, but not yet accepted as the full application runtime.

Tokio remains the default production runtime until these are proven:

- HTTP ingress can run without axum's Tokio-bound serving path, or axum is replaced at the boundary.
- Raft transport can avoid tonic or isolate tonic on Tokio bridge threads.
- Operational workers have non-blocking or explicitly isolated blocking behavior.
- Hot-shard imbalance handling exists so thread-per-core does not strand work on one core.

## Experiment Plan

A monoio experiment should be separate from the production server path:

1. Build a toy shard actor using monoio and local channels.
2. Instantiate one OpenRaft group with `openraft-rt-monoio` and `single-threaded` enabled.
3. Propose synthetic append commands through a mailbox, not shared handles.
4. Measure tail latency under balanced and intentionally skewed stream distributions.
5. Compare against an equivalent Tokio current-thread actor.

Exit criteria for adoption:

- Clear latency or CPU efficiency improvement under durable-stream-shaped workloads.
- No loss of debuggability or operational visibility.
- No forced rewrite of the public HTTP API before the shard core is stable.
