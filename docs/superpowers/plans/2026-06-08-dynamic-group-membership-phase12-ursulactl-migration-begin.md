# Dynamic Group Membership Phase 12 Ursulactl Migration Begin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual-first `ursulactl` command that starts a group migration intent through the meta admin endpoint.

**Architecture:** Reuse Phase 11 `POST /__ursula/admin/groups/{raft_group_id}/migrations`. Add a `MetricsClient::begin_migration` helper and expose it as `ursulactl group migration begin --admin-url ... --raft-group-id ... --target-voters ... [--retain-removed]`. The command only records the durable meta control-plane intent; it does not execute data-group membership changes.

**Tech Stack:** Rust, clap, reqwest, `ursula-ctl`.

---

## File Structure

- `crates/ursula-ctl/src/metrics.rs`: add begin migration response DTO, path helper, and HTTP client method.
- `crates/ursula-ctl/src/bin/ursulactl.rs`: add `group migration begin` command parsing and runner.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add path helper test**

Add `begin_migration_path_builds_admin_query` in `metrics.rs`:

```rust
#[test]
fn begin_migration_path_builds_admin_query() {
    assert_eq!(
        begin_migration_path(2, &BTreeSet::from([2, 3, 4]), true),
        "/__ursula/admin/groups/2/migrations?target_voters=2,3,4&retain_removed=true"
    );
}
```

- [x] **Step 2: Add clap parse test**

Add `parses_group_migration_begin_command` in `ursulactl.rs`:

```rust
let cli = Cli::try_parse_from([
    "ursulactl",
    "group",
    "migration",
    "begin",
    "--admin-url",
    "http://node1:4491",
    "--raft-group-id",
    "2",
    "--target-voters",
    "2,3,4",
    "--retain-removed",
])
.expect("parse group migration begin command");

let Command::Group(GroupCommand::Migration(GroupMigrationCommand::Begin(args))) = cli.command else {
    panic!("expected group migration begin command");
};
assert_eq!(args.admin_url.as_str(), "http://node1:4491/");
assert_eq!(args.raft_group_id, 2);
assert_eq!(args.target_voters, vec![2, 3, 4]);
assert!(args.retain_removed);
assert_eq!(args.http_timeout_secs, 10);
```

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-ctl begin_migration
```

Expected: compile failure or test failure because the path helper and command do not exist yet.

### Task 2: Metrics Client Support

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`

- [x] **Step 1: Add response DTO**

Add:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct BeginMigrationResponse {
    pub raft_group_id: u64,
    pub migration_id: u64,
    pub started: bool,
}
```

- [x] **Step 2: Add path helper and client method**

Add `begin_migration_path(...)` and `MetricsClient::begin_migration(...)` that posts the route, parses success JSON, and returns an error with status/body on non-success.

- [x] **Step 3: Run path helper test**

Run:

```bash
cargo test -p ursula-ctl begin_migration_path
```

Expected: path helper test passes once the CLI compile errors are resolved.

### Task 3: CLI Command

**Files:**
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add command enums and args**

Add `GroupCommand::Migration(GroupMigrationCommand)`, `GroupMigrationCommand::Begin(GroupMigrationBeginArgs)`, and args fields `admin_url`, `raft_group_id`, `target_voters: Vec<u64>`, `retain_removed`, `http_timeout_secs`.

- [x] **Step 2: Add command runner**

Add `run_group_migration_begin_subcommand` that calls `MetricsClient::begin_migration`, verifies `started`, and prints `group N: migration M started`.

- [x] **Step 3: Run clap parse test**

Run:

```bash
cargo test -p ursula-ctl parses_group_migration_begin_command
```

Expected: parse test passes.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-ctl begin_migration
cargo test -p ursula-ctl parses_group_migration_begin_command
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase12-ursulactl-migration-begin.md crates/ursula-ctl/src/metrics.rs crates/ursula-ctl/src/bin/ursulactl.rs
git commit -m "feat(ursula-ctl): add group migration begin command"
```
