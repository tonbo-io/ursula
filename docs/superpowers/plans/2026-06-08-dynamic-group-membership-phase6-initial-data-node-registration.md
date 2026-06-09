# Dynamic Group Membership Phase 6 Initial Data Node Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable meta raft helper for explicitly registering bootstrap nodes as data-capable nodes.

**Architecture:** Keep the actual persisted transition as `ControlCommand::RegisterNode`. Add a small `MetaNodeRegistration` DTO in `ursula-raft::meta` and helper methods on `MetaRaftHandle` that submit one or more registrations through OpenRaft. The batch helper treats a rejected registration as a meta bootstrap error so startup code can fail fast instead of silently running with missing data-capable nodes.

**Tech Stack:** Rust, Tokio tests, OpenRaft, `ursula-control`, `ursula-raft`.

---

## File Structure

- `crates/ursula-raft/src/meta.rs`: add `MetaNodeRegistration`, `register_node`, and `register_initial_data_nodes`.
- `crates/ursula-raft/src/lib.rs`: export `MetaNodeRegistration`.
- `crates/ursula-raft/src/tests.rs`: add tests for successful initial data-node registration and rejection handling.

### Task 1: Initial Registration Tests

**Files:**
- Modify: `crates/ursula-raft/src/tests.rs`

- [x] **Step 1: Write failing success test**

Add a test that bootstraps a single-node meta raft handle, calls:

```rust
handle
    .register_initial_data_nodes(
        [
            MetaNodeRegistration::new(1, "http://node1:4491", "http://node1:4492"),
            MetaNodeRegistration::new(2, "http://node2:4491", "http://node2:4492"),
            MetaNodeRegistration::new(3, "http://node3:4491", "http://node3:4492"),
        ],
        10,
    )
    .await
    .expect("register initial data nodes");
```

Then use `read_state` to assert nodes 1, 2, and 3 are registered with normalized URLs and `Active` state.

- [x] **Step 2: Write failing rejection test**

Add a test that calls `register_initial_data_nodes` with one registration whose `client_url` is whitespace, and assert the returned `MetaRaftError` display string contains `node 2 rejected: client_url must not be empty`.

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-raft initial_data_nodes
```

Expected: compile failure because `MetaNodeRegistration` and `register_initial_data_nodes` do not exist yet.

### Task 2: Registration Helper Implementation

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`
- Modify: `crates/ursula-raft/src/lib.rs`

- [x] **Step 1: Add `MetaNodeRegistration`**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaNodeRegistration {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub labels: BTreeMap<String, String>,
}
```

Add `new`, `with_labels`, and `into_command(now_ms)` methods.

- [x] **Step 2: Add handle helpers**

```rust
pub async fn register_node(
    &self,
    registration: MetaNodeRegistration,
    now_ms: u64,
) -> Result<ControlResponse, MetaRaftError>

pub async fn register_initial_data_nodes(
    &self,
    registrations: impl IntoIterator<Item = MetaNodeRegistration>,
    now_ms: u64,
) -> Result<(), MetaRaftError>
```

`register_initial_data_nodes` returns `Ok(())` only when every response is `ControlResponse::Ok`; rejected or unexpected responses become `MetaRaftError::new("register initial data-capable node", ...)`.

- [x] **Step 3: Export registration DTO**

Export `MetaNodeRegistration` from `crates/ursula-raft/src/lib.rs`.

- [x] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p ursula-raft initial_data_nodes
```

Expected: both new tests pass.

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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase6-initial-data-node-registration.md crates/ursula-raft/src/meta.rs crates/ursula-raft/src/lib.rs crates/ursula-raft/src/tests.rs
git commit -m "feat(ursula-raft): register initial data nodes in meta"
```
