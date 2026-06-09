# Dynamic Group Membership Phase 8 Ursulactl Node Register Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ursulactl node register` so operators can explicitly register a data-capable node through the meta-group admin endpoint.

**Architecture:** Reuse the existing `MetricsClient` HTTP wrapper for small admin POSTs. Add a nested `node register` clap command that targets one admin/meta endpoint URL and submits `node_id`, `client_url`, and `cluster_url`. Keep labels out of this first CLI slice because the server route currently accepts query parameters only.

**Tech Stack:** Rust, clap, reqwest, Tokio tests, `ursula-ctl`.

---

## File Structure

- `crates/ursula-ctl/src/metrics.rs`: add register-node request helper and response DTO.
- `crates/ursula-ctl/src/bin/ursulactl.rs`: add `node register` subcommand and runner.

### Task 1: CLI and Client Tests

**Files:**
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`
- Modify: `crates/ursula-ctl/src/metrics.rs`

- [x] **Step 1: Write failing clap parse test**

Add a bin unit test that parses:

```text
ursulactl node register \
  --admin-url http://node1:4491 \
  --node-id 5 \
  --client-url http://node5:4491 \
  --cluster-url http://node5:4492
```

Assert it produces `Command::Node(NodeCommand::Register(...))` with the supplied values.

- [x] **Step 2: Write failing register path test**

Add a `metrics.rs` unit test for:

```rust
register_node_path(5, "http://node5:4491", "http://node5:4492")
```

Expected:

```text
/__ursula/admin/nodes/5/register?client_url=http://node5:4491&cluster_url=http://node5:4492
```

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula-ctl node_register
```

Expected: compile failures because the command and helper do not exist yet.

### Task 2: CLI and Client Implementation

**Files:**
- Modify: `crates/ursula-ctl/src/metrics.rs`
- Modify: `crates/ursula-ctl/src/bin/ursulactl.rs`

- [x] **Step 1: Add HTTP client method**

Add:

```rust
pub async fn register_node(
    &self,
    admin_url: &Url,
    node_id: u64,
    client_url: &str,
    cluster_url: &str,
) -> Result<RegisterNodeResponse>
```

Use the existing request/error style from `transfer_leader` and `allow_next_revert`.

- [x] **Step 2: Add CLI command**

Add:

```rust
Command::Node(NodeCommand)
NodeCommand::Register(NodeRegisterArgs)
```

`NodeRegisterArgs` fields:

```rust
admin_url: Url
node_id: u64
client_url: String
cluster_url: String
http_timeout_secs: u64
```

- [x] **Step 3: Add runner**

Create a `MetricsClient`, call `register_node`, print:

```text
node 5: registered
```

- [x] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p ursula-ctl node_register
```

Expected: both tests pass.

### Task 3: Regression Checks

**Files:**
- Modify: none

- [x] **Step 1: Run focused ctl tests**

Run:

```bash
cargo test -p ursula-ctl node_register
```

Expected: both node-register tests pass.

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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase8-ursulactl-node-register.md crates/ursula-ctl/src/metrics.rs crates/ursula-ctl/src/bin/ursulactl.rs
git commit -m "feat(ursula-ctl): add node register command"
```
