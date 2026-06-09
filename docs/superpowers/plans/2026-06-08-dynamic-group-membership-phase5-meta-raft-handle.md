# Dynamic Group Membership Phase 5 Meta Raft Handle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable meta raft handle that bootstraps a local meta group, submits control commands, reads the applied control state, and shuts down cleanly.

**Architecture:** Keep OpenRaft details inside `crates/ursula-raft/src/meta.rs`. The handle wraps `Raft<MetaRaftTypeConfig, MetaRaftStateMachine>`, accepts injected log store and network implementations for tests/future production wiring, and provides a single-node helper for bootstrap tests. A small `MetaRaftError` keeps this boundary separate from data-group `GroupEngineError`.

**Tech Stack:** Rust, Tokio tests, OpenRaft, `ursula-control`, `ursula-raft`.

---

## File Structure

- `crates/ursula-raft/src/meta.rs`: add `MetaRaft` alias, `MetaRaftHandle`, and `MetaRaftError`.
- `crates/ursula-raft/src/lib.rs`: export the new meta handle and error.
- `crates/ursula-raft/src/tests.rs`: update the meta single-node bootstrap test to use the handle instead of constructing OpenRaft directly.

### Task 1: Handle-Driven Bootstrap Test

**Files:**
- Modify: `crates/ursula-raft/src/tests.rs`

- [x] **Step 1: Write the failing test**

Change `single_node_meta_raft_applies_node_registration` so it creates the meta group through:

```rust
let handle = MetaRaftHandle::new_single_node_with_log_store(
    1,
    BasicNode::new("meta-local"),
    config,
    log_store.clone(),
)
.await
.expect("create single-node meta raft handle");
```

Then use:

```rust
let registered = handle.write(ControlCommand::RegisterNode { ... }).await?;
let updated = handle.write(ControlCommand::SetNodeState { ... }).await?;
let (applied, node) = handle.with_state_machine(|state_machine| { ... }).await?;
handle.shutdown().await?;
```

- [x] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p ursula-raft single_node_meta_raft_applies_node_registration
```

Expected: compile failure because `MetaRaftHandle` does not exist yet.

### Task 2: Meta Raft Handle API

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`
- Modify: `crates/ursula-raft/src/lib.rs`

- [x] **Step 1: Implement the handle and error**

Add:

```rust
pub type MetaRaft = openraft::Raft<MetaRaftTypeConfig, MetaRaftStateMachine>;

#[derive(Debug)]
pub struct MetaRaftError { ... }

#[derive(Clone)]
pub struct MetaRaftHandle {
    raft: MetaRaft,
}
```

Methods:

```rust
new_node_with_log_store_and_network(...)
new_single_node_with_log_store(...)
initialize_membership(...)
wait_for_current_leader(...)
raft_handle()
write(...)
with_state_machine(...)
read_state(...)
shutdown(...)
```

Each method maps OpenRaft errors into `MetaRaftError` with operation context and preserves the source.

- [x] **Step 2: Export the handle**

Export these from `crates/ursula-raft/src/lib.rs`:

```rust
pub use meta::MetaRaft;
pub use meta::MetaRaftError;
pub use meta::MetaRaftHandle;
```

- [x] **Step 3: Run test to verify it passes**

Run:

```bash
cargo test -p ursula-raft single_node_meta_raft_applies_node_registration
```

Expected: the handle-driven bootstrap test passes.

### Task 3: Regression Checks

**Files:**
- Modify: none

- [x] **Step 1: Run focused meta tests**

Run:

```bash
cargo test -p ursula-raft meta
```

Expected: all meta tests pass.

- [x] **Step 2: Run formatting and lint checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Expected: all checks exit successfully.

- [x] **Step 3: Commit**

Run:

```bash
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase5-meta-raft-handle.md crates/ursula-raft/src/meta.rs crates/ursula-raft/src/lib.rs crates/ursula-raft/src/tests.rs
git commit -m "feat(ursula-raft): add meta raft handle"
```
