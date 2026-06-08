# Dynamic Group Membership Phase 14 Ursulactl Local Engine Prepare Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual-first `ursulactl` command that prepares the local node to host a data-group raft engine.

**Architecture:** Reuse Phase 13 `POST /__ursula/admin/groups/{raft_group_id}/local-engine`. Add a `MetricsClient::prepare_local_engine` helper and expose it as `ursulactl group local-engine prepare --admin-url ... --raft-group-id ...`. This command runs on the target node's admin URL before the operator adds that node as a learner.

**Tech Stack:** Rust, clap, reqwest, `ursula-ctl`.

---

## File Structure

- `crates/ursula-ctl/src/metrics.rs`: add prepare-local-engine response DTO, path helper, and HTTP client method.
- `crates/ursula-ctl/src/bin/ursulactl.rs`: add `group local-engine prepare` command parsing and runner.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add path helper test**

Add `prepare_local_engine_path_builds_admin_path` in `metrics.rs`:

```rust
#[test]
fn prepare_local_engine_path_builds_admin_path() {
    assert_eq!(
        prepare_local_engine_path(2),
        "/__ursula/admin/groups/2/local-engine"
    );
}
```

- [x] **Step 2: Add clap parse test**

Add `parses_group_local_engine_prepare_command` in `ursulactl.rs`:

```rust
let cli = Cli::try_parse_from([
    "ursulactl",
    "group",
    "local-engine",
    "prepare",
    "--admin-url",
    "http://node4:4491",
    "--raft-group-id",
    "2",
])
.expect("parse group local-engine prepare command");

let Command::Group(GroupCommand::LocalEngine(GroupLocalEngineCommand::Prepare(args))) = cli.command else {
    panic!("expected group local-engine prepare command");
};
assert_eq!(args.admin_url.as_str(), "http://node4:4491/");
assert_eq!(args.raft_group_id, 2);
assert_eq!(args.http_timeout_secs, 10);
```

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-ctl prepare_local_engine
cargo test -p ursula-ctl parses_group_local_engine_prepare_command
```

Expected: compile failure or test failure because the path helper and command do not exist yet.

### Task 2: Metrics Client Support

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`

- [x] **Step 1: Add response DTO**

Add:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct PrepareLocalEngineResponse {
    pub raft_group_id: u64,
    pub core_id: u64,
    pub prepared: bool,
    pub already_allowed: bool,
}
```

- [x] **Step 2: Add path helper and client method**

Add `prepare_local_engine_path(raft_group_id)` and `MetricsClient::prepare_local_engine(&self, admin_url, raft_group_id)` that posts the route, parses success JSON, and returns an error with status/body on non-success.

- [x] **Step 3: Run path helper test**

Run:

```bash
cargo test -p ursula-ctl prepare_local_engine_path
```

Expected: path helper test passes once the CLI compile errors are resolved.

### Task 3: CLI Command

**Files:**
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add command enums and args**

Add `GroupCommand::LocalEngine(GroupLocalEngineCommand)`, `GroupLocalEngineCommand::Prepare(GroupLocalEnginePrepareArgs)`, and args fields `admin_url`, `raft_group_id`, `http_timeout_secs`.

- [x] **Step 2: Add command runner**

Add `run_group_local_engine_prepare_subcommand` that calls `MetricsClient::prepare_local_engine`, verifies `prepared`, and prints `group N: local engine prepared on core C`.

- [x] **Step 3: Run clap parse test**

Run:

```bash
cargo test -p ursula-ctl parses_group_local_engine_prepare_command
```

Expected: parse test passes.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-ctl prepare_local_engine
cargo test -p ursula-ctl parses_group_local_engine_prepare_command
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase14-ursulactl-local-engine-prepare.md crates/ursula-ctl/src/metrics.rs crates/ursula-ctl/src/bin/ursulactl.rs
git commit -m "feat(ursula-ctl): add local engine prepare command"
```
