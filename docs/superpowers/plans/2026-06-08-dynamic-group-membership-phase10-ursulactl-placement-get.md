# Dynamic Group Membership Phase 10 Ursulactl Placement Get Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual-first `ursulactl` command that reads a data-group placement projection from the meta admin endpoint.

**Architecture:** Reuse the Phase 9 HTTP endpoint `GET /__ursula/admin/groups/{raft_group_id}/placement`. Add a `MetricsClient::group_placement` helper with a narrow response DTO local to `ursula-ctl`, then expose it as `ursulactl group placement get --admin-url ... --raft-group-id ...`. The command prints the placement JSON so operators can inspect membership before planning a migration.

**Tech Stack:** Rust, clap, reqwest, serde_json, `ursula-ctl`.

---

## File Structure

- `crates/ursula-ctl/src/metrics.rs`: add group placement response DTO, path helper, and HTTP client method.
- `crates/ursula-ctl/src/bin/ursulactl.rs`: add `group placement get` command parsing and runner.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add path helper test**

Add `group_placement_path_builds_admin_projection_path` in `metrics.rs`:

```rust
#[test]
fn group_placement_path_builds_admin_projection_path() {
    assert_eq!(
        group_placement_path(7),
        "/__ursula/admin/groups/7/placement"
    );
}
```

- [x] **Step 2: Add clap parse test**

Add `parses_group_placement_get_command` in `ursulactl.rs`:

```rust
let cli = Cli::try_parse_from([
    "ursulactl",
    "group",
    "placement",
    "get",
    "--admin-url",
    "http://node1:4491",
    "--raft-group-id",
    "7",
])
.expect("parse group placement get command");

let Command::Group(GroupCommand::Placement(GroupPlacementCommand::Get(args))) = cli.command else {
    panic!("expected group placement get command");
};
assert_eq!(args.admin_url.as_str(), "http://node1:4491/");
assert_eq!(args.raft_group_id, 7);
assert_eq!(args.http_timeout_secs, 10);
```

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-ctl group_placement
```

Expected: compile failure or test failure because the command and path helper do not exist yet.

### Task 2: Metrics Client Support

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`

- [x] **Step 1: Add response DTOs**

Add serializable/deserializable local DTOs:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupPlacementResponse {
    pub raft_group_id: u64,
    pub voters: BTreeSet<u64>,
    pub learners: BTreeSet<u64>,
    pub draining: BTreeSet<u64>,
    pub epoch: u64,
    pub nodes: BTreeMap<u64, PlacementNodeResponse>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlacementNodeResponse {
    pub node_id: u64,
    pub client_url: String,
    pub cluster_url: String,
    pub state: String,
}
```

- [x] **Step 2: Add path helper and client method**

Add:

```rust
fn group_placement_path(raft_group_id: u64) -> String {
    format!("/__ursula/admin/groups/{raft_group_id}/placement")
}
```

Add `MetricsClient::group_placement(&self, admin_url: &Url, raft_group_id: u64) -> Result<GroupPlacementResponse>` that performs `GET`, parses success JSON, and returns an error with status/body on non-success.

- [x] **Step 3: Run path helper test**

Run:

```bash
cargo test -p ursula-ctl group_placement_path
```

Expected: path helper test passes.

### Task 3: CLI Command

**Files:**
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add command enums and args**

Add `Command::Group(GroupCommand)`, `GroupCommand::Placement(GroupPlacementCommand)`, `GroupPlacementCommand::Get(GroupPlacementGetArgs)`, and args fields `admin_url`, `raft_group_id`, `http_timeout_secs`.

- [x] **Step 2: Add command runner**

Add `run_group_placement_get_subcommand` that calls `MetricsClient::group_placement`, writes pretty JSON to stdout, and appends a newline.

- [x] **Step 3: Run clap parse test**

Run:

```bash
cargo test -p ursula-ctl parses_group_placement_get_command
```

Expected: parse test passes.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-ctl group_placement
cargo test -p ursula-ctl parses_group_placement_get_command
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase10-ursulactl-placement-get.md crates/ursula-ctl/src/metrics.rs crates/ursula-ctl/src/bin/ursulactl.rs
git commit -m "feat(ursula-ctl): add group placement get command"
```
