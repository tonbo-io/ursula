# Dynamic Group Membership Phase 13 Admin Prepare Local Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator explicitly prepare a target node to host a data-group raft engine before adding it as a learner.

**Architecture:** Keep static per-group routing behavior unchanged by default. Add a dynamic host whitelist to `RaftGroupHandleRegistry`; `StaticGrpcRaftGroupEngineFactory::hosts_group` allows a non-voter group only after that group is whitelisted locally. Add `POST /__ursula/admin/groups/{raft_group_id}/local-engine` that whitelists the group on this node and calls `ShardRuntime::warm_group`.

**Tech Stack:** Rust, axum, OpenRaft, `ursula`, `ursula-raft`, `ursula-runtime`.

---

## File Structure

- `crates/ursula-raft/src/registry.rs`: add dynamic local-host whitelist methods.
- `crates/ursula-raft/src/engine/factory.rs`: allow `hosts_group` when the registry whitelist contains the group.
- `crates/ursula-raft/src/tests.rs`: add a factory/registry unit test for non-voter dynamic hosting.
- `crates/ursula/src/lib.rs`: add the admin route and handler that prepares/warm the local engine.
- `crates/ursula/src/tests.rs`: add HTTP route tests for local engine preparation and missing registry.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula-raft/src/tests.rs`
- Modify: `crates/ursula/src/tests.rs`

- [x] **Step 1: Add factory dynamic host test**

Add `dynamic_group_hosting_allows_non_voter_warmup` in `ursula-raft` tests:

```rust
let registry = RaftGroupHandleRegistry::default();
let factory = StaticGrpcRaftGroupEngineFactory::new(
    4,
    [(1, "node1".to_owned()), (2, "node2".to_owned()), (3, "node3".to_owned()), (4, "node4".to_owned())],
    false,
    registry.clone(),
)
.with_per_group_voters(BTreeMap::from([(RaftGroupId(2), BTreeSet::from([1, 2, 3]))]));
let placement = ShardPlacement { core_id: CoreId(0), raft_group_id: RaftGroupId(2) };
assert!(!factory.hosts_group(placement));
registry.allow_dynamic_group_hosting(RaftGroupId(2));
assert!(factory.hosts_group(placement));
```

- [x] **Step 2: Add HTTP prepare test**

Add `admin_prepare_local_engine_warms_dynamically_allowed_group` in `ursula` tests. Spawn a `ShardRuntime` with a `StaticGrpcRaftGroupEngineFactory` for node 4 and per-group voters `[1,2,3]`, mount `HttpState::with_raft_registry`, call `POST /__ursula/admin/groups/2/local-engine`, assert `200`, response JSON has `prepared: true`, and registry contains group 2.

- [x] **Step 3: Add missing-registry HTTP test**

Add `admin_prepare_local_engine_requires_raft_registry`, using ordinary `HttpState::new(...)` and asserting 400 with `raft registry is not configured for this server`.

- [x] **Step 4: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-raft dynamic_group_hosting_allows_non_voter_warmup
cargo test -p ursula admin_prepare_local_engine
```

Expected: compile failure or route failure because registry whitelist and admin endpoint do not exist yet.

### Task 2: Registry And Factory Support

**Files:**
- Modify: `crates/ursula-raft/src/registry.rs`
- Modify: `crates/ursula-raft/src/engine/factory.rs`

- [x] **Step 1: Add registry whitelist state**

Add `dynamic_hosted_groups: Arc<Mutex<BTreeSet<RaftGroupId>>>` to `RaftGroupHandleRegistry`, initialize it in `Default`, and add:

```rust
pub fn allow_dynamic_group_hosting(&self, raft_group_id: RaftGroupId) -> bool
pub fn dynamic_group_hosting_allowed(&self, raft_group_id: RaftGroupId) -> bool
```

- [x] **Step 2: Update static factory hosts_group**

In `StaticGrpcRaftGroupEngineFactory::hosts_group`, keep current behavior for all-voter mode and configured voters, then return `self.registry.dynamic_group_hosting_allowed(placement.raft_group_id)` for non-voters.

- [x] **Step 3: Run factory test**

Run:

```bash
cargo test -p ursula-raft dynamic_group_hosting_allows_non_voter_warmup
```

Expected: test passes.

### Task 3: HTTP Prepare Endpoint

**Files:**
- Modify: `crates/ursula/src/lib.rs`

- [x] **Step 1: Mount route**

Add:

```rust
.route(
    "/__ursula/admin/groups/{raft_group_id}/local-engine",
    post(admin_prepare_local_engine),
)
```

- [x] **Step 2: Implement handler**

Parse `raft_group_id`, require `state.raft_registry()`, call `registry.allow_dynamic_group_hosting(raft_group_id)`, then `state.runtime().warm_group(raft_group_id).await`.

Return:

- `200 application/json` with `{"raft_group_id":N,"core_id":C,"prepared":true,"already_allowed":B}`
- `400` for invalid group id or missing registry
- `500` for warmup errors

- [x] **Step 3: Run HTTP tests**

Run:

```bash
cargo test -p ursula admin_prepare_local_engine
```

Expected: HTTP tests pass.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-raft dynamic_group_hosting_allows_non_voter_warmup
cargo test -p ursula admin_prepare_local_engine
```

Expected: focused tests pass.

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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase13-admin-prepare-local-engine.md crates/ursula-raft/src/registry.rs crates/ursula-raft/src/engine/factory.rs crates/ursula-raft/src/tests.rs crates/ursula/src/lib.rs crates/ursula/src/tests.rs
git commit -m "feat(ursula): add admin local engine preparation"
```
