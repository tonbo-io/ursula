# Dynamic Group Membership Phase 11 Admin Begin Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator start exactly one group migration intent by writing `BeginMigration` into the meta group through the admin HTTP API.

**Architecture:** Reuse `ControlCommand::BeginMigration` and the existing `ControlPlaneState` active-migration lock. Add a small `MetaRaftHandle::begin_migration` wrapper, then expose `POST /__ursula/admin/groups/{raft_group_id}/migrations?target_voters=...&retain_removed=...`. This does not call data-group OpenRaft membership APIs yet; it only records the durable control-plane intent.

**Tech Stack:** Rust, axum, OpenRaft, `ursula`, `ursula-raft`, `ursula-control`.

---

## File Structure

- `crates/ursula-raft/src/meta.rs`: add a typed `MetaRaftHandle::begin_migration` helper.
- `crates/ursula/src/lib.rs`: add the admin route, query parsing, and handler.
- `crates/ursula/src/tests.rs`: add HTTP route tests for successful begin and missing meta group.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula/src/tests.rs`

- [x] **Step 1: Add successful begin migration HTTP test**

Add `admin_begin_migration_writes_meta_migration`:

```rust
let now_ms = Arc::new(AtomicU64::new(2_000));
let meta = single_node_meta_handle_for_test("ursula-admin-begin-migration-test").await;
meta.register_initial_data_nodes([...nodes 1,2,3,4...], 10).await?;
meta.write(ursula_control::ControlCommand::SeedPlacement {
    raft_group_id: RaftGroupId(2),
    voters: BTreeSet::from([1, 2, 3]),
    now_ms: 20,
}).await?;

let state = HttpState::new(spawn_default_runtime(1, 1).expect("runtime"))
    .with_meta_raft_handle(meta.clone())
    .with_wall_clock(TestWallClock { now_ms: Arc::clone(&now_ms) });
let response = client_router_from_state(state)
    .oneshot(POST /__ursula/admin/groups/2/migrations?target_voters=2,3,4&retain_removed=true)
    .await?;
assert_eq!(response.status(), StatusCode::OK);
```

Assert the response JSON contains `raft_group_id: 2`, `migration_id: 1`, and `started: true`. Then read meta state and assert the active migration targets voters `[2,3,4]`, retains removed voters, and has `created_at_ms == 2_000`.

- [x] **Step 2: Add missing-meta test**

Add `admin_begin_migration_requires_meta_group`, using ordinary `HttpState::new(...)` and asserting the route returns 400 with `meta raft is not configured for this server`.

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula admin_begin_migration
```

Expected: route failure or compile failure because the admin migration endpoint does not exist yet.

### Task 2: MetaRaftHandle Wrapper

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`

- [x] **Step 1: Add begin_migration helper**

Add:

```rust
pub async fn begin_migration(
    &self,
    raft_group_id: RaftGroupId,
    target_voters: BTreeSet<u64>,
    retain_removed: bool,
    now_ms: u64,
) -> Result<ControlResponse, MetaRaftError> {
    self.write(ControlCommand::BeginMigration {
        raft_group_id,
        target_voters,
        retain_removed,
        now_ms,
    })
    .await
}
```

### Task 3: HTTP Handler

**Files:**
- Modify: `crates/ursula/src/lib.rs`

- [x] **Step 1: Mount route**

Add:

```rust
.route(
    "/__ursula/admin/groups/{raft_group_id}/migrations",
    post(admin_begin_migration),
)
```

- [x] **Step 2: Parse query and write meta command**

Require `target_voters`, parse it with `parse_voter_ids`, parse optional `retain_removed` with default `false`, require `state.meta_raft()`, then call `meta_raft.begin_migration(...)`.

Return:

- `200 application/json` with `{"raft_group_id":N,"migration_id":M,"started":true}`
- `400` for bad query, missing meta group, or control-plane rejection
- `500` for unexpected responses or meta write errors

- [x] **Step 3: Run tests to verify they pass**

Run:

```bash
cargo test -p ursula admin_begin_migration
```

Expected: both tests pass.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula admin_begin_migration
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase11-admin-begin-migration.md crates/ursula-raft/src/meta.rs crates/ursula/src/lib.rs crates/ursula/src/tests.rs
git commit -m "feat(ursula): add admin begin migration route"
```
