# Dynamic Group Membership Phase 16 Admin Commit Finish Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator commit the observed data-group placement into the meta group and finish the active migration.

**Architecture:** Reuse `ControlCommand::CommitPlacement` and `ControlCommand::FinishMigration`. Add typed `MetaRaftHandle` helpers, then expose `POST /__ursula/admin/groups/{raft_group_id}/placement/commit` and `POST /__ursula/admin/migrations/{migration_id}/finish`. This completes the manual control-plane loop after data-group membership has been changed through OpenRaft.

**Tech Stack:** Rust, axum, OpenRaft, `ursula`, `ursula-raft`, `ursula-control`.

---

## File Structure

- `crates/ursula-raft/src/meta.rs`: add typed helpers for `CommitPlacement` and `FinishMigration`.
- `crates/ursula/src/lib.rs`: add admin routes, query parsing, and handlers.
- `crates/ursula/src/tests.rs`: add HTTP route tests for commit and finish.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula/src/tests.rs`

- [x] **Step 1: Add placement commit HTTP test**

Add `admin_commit_placement_writes_meta_projection`:

```rust
let meta = single_node_meta_handle_for_test("ursula-admin-commit-placement-test").await;
meta.register_initial_data_nodes([...nodes 1,2,3,4...], 10).await?;
meta.write(ControlCommand::SeedPlacement { raft_group_id: RaftGroupId(2), voters: BTreeSet::from([1,2,3]), now_ms: 20 }).await?;
POST /__ursula/admin/groups/2/placement/commit?voters=2,3,4&learners=1&draining=1
```

Assert `200`, JSON has `raft_group_id: 2` and `committed: true`, and meta placement now has voters `[2,3,4]`, learners `[1]`, draining `[1]`, epoch `1`, and `updated_at_ms` from the test wall clock.

- [x] **Step 2: Add finish migration HTTP test**

Add `admin_finish_migration_releases_meta_lock`:

```rust
seed placement, begin migration, then:
POST /__ursula/admin/migrations/1/finish?success=true
```

Assert `200`, JSON has `migration_id: 1`, `success: true`, `finished: true`, meta `active_migration` is `None`, and migration phase is `Succeeded`.

- [x] **Step 3: Add missing-meta tests**

Add missing-meta assertions for both routes returning 400 with `meta raft is not configured for this server`.

- [x] **Step 4: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula admin_commit_placement
cargo test -p ursula admin_finish_migration
```

Expected: route failure or compile failure because the admin endpoints do not exist yet.

### Task 2: MetaRaftHandle Helpers

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`

- [x] **Step 1: Add commit_placement helper**

Add `MetaRaftHandle::commit_placement(raft_group_id, voters, learners, draining, now_ms)` that writes `ControlCommand::CommitPlacement`.

- [x] **Step 2: Add finish_migration helper**

Add `MetaRaftHandle::finish_migration(migration_id, success, now_ms)` that writes `ControlCommand::FinishMigration`.

### Task 3: HTTP Handlers

**Files:**
- Modify: `crates/ursula/src/lib.rs`

- [x] **Step 1: Mount routes**

Add:

```rust
.route(
    "/__ursula/admin/groups/{raft_group_id}/placement/commit",
    post(admin_commit_placement),
)
.route(
    "/__ursula/admin/migrations/{migration_id}/finish",
    post(admin_finish_migration),
)
```

- [x] **Step 2: Implement query parsing helpers**

Add `parse_optional_node_ids_query` for optional `learners` and `draining`, reusing `parse_voter_ids` when a non-empty value is supplied.

- [x] **Step 3: Implement handlers**

`admin_commit_placement` parses required `voters`, optional `learners` and `draining`, requires meta, writes `commit_placement`, maps `Ok` to JSON and `Rejected` to 400.

`admin_finish_migration` parses optional `success` with default `true`, requires meta, writes `finish_migration`, maps `Ok` to JSON and `Rejected` to 400.

- [x] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p ursula admin_commit_placement
cargo test -p ursula admin_finish_migration
```

Expected: tests pass.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula admin_commit_placement
cargo test -p ursula admin_finish_migration
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase16-admin-commit-finish-migration.md crates/ursula-raft/src/meta.rs crates/ursula/src/lib.rs crates/ursula/src/tests.rs
git commit -m "feat(ursula): add admin migration commit finish routes"
```
