# Dynamic Group Membership Phase 9 Admin Placement Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a read-only HTTP admin projection for data-group placement stored in the meta group.

**Architecture:** Reuse `ControlPlaneState::placement_view` and the optional `MetaRaftHandle` already mounted in `HttpState`. Add `GET /__ursula/admin/groups/{raft_group_id}/placement`, returning the serialized `GroupPlacementView` from the meta state machine. This is a small read surface for operators and a future runtime placement cache without changing data-group routing yet.

**Tech Stack:** Rust, axum, serde_json, Tokio tests, OpenRaft, `ursula`, `ursula-control`.

---

## File Structure

- `crates/ursula/src/lib.rs`: add the admin placement route and handler.
- `crates/ursula/src/tests.rs`: add HTTP route tests for configured and missing meta group.

### Task 1: Admin Placement Tests

**Files:**
- Modify: `crates/ursula/src/tests.rs`

- [x] **Step 1: Write failing configured-meta test**

Add `admin_group_placement_reads_meta_projection`:

```rust
let meta = single_node_meta_handle_for_test("ursula-admin-placement-test").await;
meta.register_initial_data_nodes([...], 10).await?;
meta.write(ursula_control::ControlCommand::SeedPlacement {
    raft_group_id: RaftGroupId(2),
    voters: BTreeSet::from([1, 3]),
    now_ms: 20,
}).await?;
let state = HttpState::new(spawn_default_runtime(1, 1).expect("runtime"))
    .with_meta_raft_handle(meta.clone());
let response = client_router_from_state(state)
    .oneshot(GET /__ursula/admin/groups/2/placement)
    .await?;
assert_eq!(response.status(), StatusCode::OK);
```

Decode the JSON and assert `raft_group_id == 2`, voters `[1,3]`, and node 1/3 client URLs are present.

- [x] **Step 2: Write failing missing-meta test**

Add `admin_group_placement_requires_meta_group`, using ordinary `HttpState::new(...)` and asserting the route returns 400 with `meta raft is not configured for this server`.

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula admin_group_placement
```

Expected: route failure or compile failure because the admin placement endpoint does not exist yet.

### Task 2: Admin Placement Handler

**Files:**
- Modify: `crates/ursula/src/lib.rs`

- [x] **Step 1: Mount route**

In `client_router_from_state`, add:

```rust
.route(
    "/__ursula/admin/groups/{raft_group_id}/placement",
    get(admin_group_placement),
)
```

- [x] **Step 2: Implement handler**

Parse `raft_group_id` through `parse_raft_group_id`, require `state.meta_raft()`, then call:

```rust
meta_raft
    .read_state(move |state| state.placement_view(raft_group_id))
    .await
```

Return:

- `200 application/json` with serialized `GroupPlacementView`
- `400` when meta raft is missing or group id is invalid
- `404` when there is no placement for the group
- `500` when reading or serializing fails

- [x] **Step 3: Run tests to verify they pass**

Run:

```bash
cargo test -p ursula admin_group_placement
```

Expected: both tests pass.

### Task 3: Regression Checks

**Files:**
- Modify: none

- [x] **Step 1: Run focused Ursula tests**

Run:

```bash
cargo test -p ursula admin_group_placement
```

Expected: both admin placement projection tests pass.

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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase9-admin-placement-projection.md crates/ursula/src/lib.rs crates/ursula/src/tests.rs
git commit -m "feat(ursula): expose admin placement projection"
```
