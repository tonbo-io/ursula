# Dynamic Group Membership Phase 15 Ursulactl Raft Membership Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add manual `ursulactl` commands for the existing data-group OpenRaft learner and membership endpoints.

**Architecture:** Reuse server endpoints `POST /__ursula/raft/{raft_group_id}/learners/{node_id}?addr=...` and `POST /__ursula/raft/{raft_group_id}/membership?voters=...`. Add narrow `MetricsClient` helpers and CLI commands `group learner add` and `group membership change`. These are explicit operator steps used after local-engine preparation and before meta placement commit.

**Tech Stack:** Rust, clap, reqwest, `ursula-ctl`.

---

## File Structure

- `crates/ursula-ctl/src/metrics.rs`: add learner/membership response DTOs, path helpers, and HTTP client methods.
- `crates/ursula-ctl/src/bin/ursulactl.rs`: add `group learner add` and `group membership change` command parsing/runners.

### Task 1: Failing Tests

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add path helper tests**

Add in `metrics.rs`:

```rust
#[test]
fn add_learner_path_preserves_raw_addr_for_server_query_parser() {
    assert_eq!(
        add_learner_path(2, 4, "http://node4:4492"),
        "/__ursula/raft/2/learners/4?addr=http://node4:4492"
    );
}

#[test]
fn change_membership_path_builds_voter_query() {
    assert_eq!(
        change_membership_path(2, &BTreeSet::from([2, 3, 4])),
        "/__ursula/raft/2/membership?voters=2,3,4"
    );
}
```

- [x] **Step 2: Add clap parse tests**

Add in `ursulactl.rs`:

```rust
let Command::Group(GroupCommand::Learner(GroupLearnerCommand::Add(args))) = cli.command else { ... };
assert_eq!(args.leader_url.as_str(), "http://node2:4491/");
assert_eq!(args.raft_group_id, 2);
assert_eq!(args.node_id, 4);
assert_eq!(args.cluster_url, "http://node4:4492");
```

And:

```rust
let Command::Group(GroupCommand::Membership(GroupMembershipCommand::Change(args))) = cli.command else { ... };
assert_eq!(args.leader_url.as_str(), "http://node2:4491/");
assert_eq!(args.raft_group_id, 2);
assert_eq!(args.voters, vec![2, 3, 4]);
```

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-ctl add_learner_path
cargo test -p ursula-ctl change_membership_path
cargo test -p ursula-ctl parses_group_learner_add_command
cargo test -p ursula-ctl parses_group_membership_change_command
```

Expected: compile failure or test failure because helpers and commands do not exist yet.

### Task 2: Metrics Client Support

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`

- [x] **Step 1: Add response DTOs**

Add:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct AddLearnerResponse {
    pub raft_group_id: u64,
    pub node_id: u64,
    pub log_index: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangeMembershipResponse {
    pub raft_group_id: u64,
    pub voter_ids: Vec<u64>,
    pub log_index: u64,
    pub changed: bool,
}
```

- [x] **Step 2: Add path helpers and client methods**

Add `add_learner_path`, `change_membership_path`, `MetricsClient::add_learner`, and `MetricsClient::change_membership`. Both methods post to the leader/admin URL, parse success JSON, and return status/body errors on non-success.

- [x] **Step 3: Run path helper tests**

Run:

```bash
cargo test -p ursula-ctl add_learner_path
cargo test -p ursula-ctl change_membership_path
```

Expected: path tests pass once CLI compile errors are resolved.

### Task 3: CLI Commands

**Files:**
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add learner command**

Add `GroupCommand::Learner(GroupLearnerCommand)`, `GroupLearnerCommand::Add(GroupLearnerAddArgs)`, and a runner that calls `MetricsClient::add_learner` and prints `group N: learner X added at log index I`.

- [x] **Step 2: Add membership command**

Add `GroupCommand::Membership(GroupMembershipCommand)`, `GroupMembershipCommand::Change(GroupMembershipChangeArgs)`, and a runner that converts voters to `BTreeSet`, calls `MetricsClient::change_membership`, verifies `changed`, and prints `group N: membership changed at log index I`.

- [x] **Step 3: Run clap parse tests**

Run:

```bash
cargo test -p ursula-ctl parses_group_learner_add_command
cargo test -p ursula-ctl parses_group_membership_change_command
```

Expected: parse tests pass.

### Task 4: Regression Checks And Commit

**Files:**
- Modify: none

- [x] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-ctl add_learner_path
cargo test -p ursula-ctl change_membership_path
cargo test -p ursula-ctl parses_group_learner_add_command
cargo test -p ursula-ctl parses_group_membership_change_command
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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase15-ursulactl-raft-membership-ops.md crates/ursula-ctl/src/metrics.rs crates/ursula-ctl/src/bin/ursulactl.rs
git commit -m "feat(ursula-ctl): add raft membership commands"
```
