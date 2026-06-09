# Dynamic Group Membership Phase 17 Ursulactl Commit Finish Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add manual `ursulactl` commands that commit final placement to meta and finish a migration.

**Architecture:** Reuse Phase 16 admin endpoints. Add `MetricsClient::commit_placement` and `MetricsClient::finish_migration`, then expose `ursulactl group placement commit` and `ursulactl group migration finish`. This completes the CLI sequence for manual migration after OpenRaft membership changes.

**Tech Stack:** Rust, clap, reqwest, `ursula-ctl`.

---

## File Structure

- `crates/ursula-ctl/src/metrics.rs`: add response DTOs, path helpers, and HTTP client methods.
- `crates/ursula-ctl/src/bin/ursulactl.rs`: add `group placement commit` and `group migration finish` command parsing/runners.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add path helper tests**

Add tests:

```rust
commit_placement_path(2, voters=[2,3,4], learners=[1], draining=[1])
// "/__ursula/admin/groups/2/placement/commit?voters=2,3,4&learners=1&draining=1"

finish_migration_path(7, true)
// "/__ursula/admin/migrations/7/finish?success=true"
```

- [x] **Step 2: Add clap parse tests**

Add parse tests for:

```bash
ursulactl group placement commit --admin-url http://node1:4491 --raft-group-id 2 --voters 2,3,4 --learners 1 --draining 1
ursulactl group migration finish --admin-url http://node1:4491 --migration-id 7 --success
```

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-ctl commit_placement_path
cargo test -p ursula-ctl finish_migration_path
cargo test -p ursula-ctl parses_group_placement_commit_command
cargo test -p ursula-ctl parses_group_migration_finish_command
```

Expected: compile failure or test failure because helpers and commands do not exist yet.

### Task 2: Metrics Client Support

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`

- [x] **Step 1: Add response DTOs**

Add:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct CommitPlacementResponse {
    pub raft_group_id: u64,
    pub committed: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FinishMigrationResponse {
    pub migration_id: u64,
    pub success: bool,
    pub finished: bool,
}
```

- [x] **Step 2: Add path helpers and client methods**

Add `commit_placement_path`, `finish_migration_path`, `MetricsClient::commit_placement`, and `MetricsClient::finish_migration`.

- [x] **Step 3: Run path helper tests**

Run:

```bash
cargo test -p ursula-ctl commit_placement_path
cargo test -p ursula-ctl finish_migration_path
```

Expected: path tests pass once CLI compile errors are resolved.

### Task 3: CLI Commands

**Files:**
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add placement commit command**

Extend `GroupPlacementCommand` with `Commit(GroupPlacementCommitArgs)`, parse voters as `Vec<u64>`, learners/draining as optional comma-delimited `Vec<u64>`, convert to `BTreeSet`, call client, verify `committed`, and print `group N: placement committed`.

- [x] **Step 2: Add migration finish command**

Extend `GroupMigrationCommand` with `Finish(GroupMigrationFinishArgs)`, parse `--success` as a boolean flag defaulting to false, call client, verify `finished`, and print `migration N: finished success=true|false`.

- [x] **Step 3: Run clap parse tests**

Run:

```bash
cargo test -p ursula-ctl parses_group_placement_commit_command
cargo test -p ursula-ctl parses_group_migration_finish_command
```

Expected: parse tests pass.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-ctl commit_placement_path
cargo test -p ursula-ctl finish_migration_path
cargo test -p ursula-ctl parses_group_placement_commit_command
cargo test -p ursula-ctl parses_group_migration_finish_command
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase17-ursulactl-commit-finish-migration.md crates/ursula-ctl/src/metrics.rs crates/ursula-ctl/src/bin/ursulactl.rs
git commit -m "feat(ursula-ctl): add migration commit finish commands"
```
