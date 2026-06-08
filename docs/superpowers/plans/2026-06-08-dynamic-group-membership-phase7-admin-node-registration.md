# Dynamic Group Membership Phase 7 Admin Node Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose an HTTP admin endpoint that explicitly registers a data-capable node by writing to the meta group.

**Architecture:** Add an optional `MetaRaftHandle` to `HttpState`, preserving existing runtimes that do not configure a meta group. Mount a client-plane admin route at `POST /__ursula/admin/nodes/{node_id}/register?client_url=...&cluster_url=...`. The route validates required query parameters, writes `MetaNodeRegistration` through the meta raft handle, and converts control-plane rejections into 400 responses.

**Tech Stack:** Rust, axum, Tokio tests, OpenRaft, `ursula`, `ursula-raft`.

---

## File Structure

- `crates/ursula/src/lib.rs`: add optional meta raft state, route, and handler.
- `crates/ursula/src/tests.rs`: add HTTP admin route tests.
- `crates/ursula/Cargo.toml`: add direct `ursula-control` dependency for matching `ControlResponse`.
- `Cargo.lock`: record the direct `ursula-control` dependency for `ursula`.

### Task 1: Admin Route Tests

**Files:**
- Modify: `crates/ursula/src/tests.rs`

- [x] **Step 1: Write failing configured-meta test**

Add `admin_register_node_writes_to_meta_group`:

```rust
let meta = MetaRaftHandle::new_single_node_with_log_store(...).await?;
let state = HttpState::new(spawn_default_runtime(1, 1).expect("runtime"))
    .with_meta_raft_handle(meta.clone())
    .with_wall_clock(TestWallClock { now_ms });
let response = client_router_from_state(state)
    .oneshot(POST /__ursula/admin/nodes/4/register?client_url=http://node4:4491/&cluster_url=http://node4:4492/)
    .await?;
assert_eq!(response.status(), StatusCode::OK);
```

Then read `meta.read_state(...)` and assert node 4 is present with normalized URLs and `updated_at_ms == now_ms`.

- [x] **Step 2: Write failing missing-meta test**

Add `admin_register_node_requires_meta_group` that uses ordinary `HttpState::new(...)` and asserts the same route returns `400` with `meta raft is not configured for this server`.

- [x] **Step 3: Run tests to verify they fail**

Run:

```bash
cargo test -p ursula admin_register_node
```

Expected: compile or route failure because `HttpState::with_meta_raft_handle` and the route do not exist yet.

### Task 2: Admin Route Implementation

**Files:**
- Modify: `crates/ursula/src/lib.rs`
- Modify: `crates/ursula/Cargo.toml`
- Modify: `Cargo.lock`

- [x] **Step 1: Add meta handle to `HttpState`**

Add:

```rust
meta_raft: Option<MetaRaftHandle>,
```

Set it to `None` in existing constructors. Add:

```rust
pub fn with_meta_raft_handle(mut self, meta_raft: MetaRaftHandle) -> Self
pub fn meta_raft(&self) -> Option<&MetaRaftHandle>
```

- [x] **Step 2: Mount route**

In `client_router_from_state`, add:

```rust
.route(
    "/__ursula/admin/nodes/{node_id}/register",
    post(register_admin_node),
)
```

- [x] **Step 3: Implement handler**

Parse `client_url` and `cluster_url` query params. If meta raft is missing, return 400. Call:

```rust
meta_raft
    .register_node(MetaNodeRegistration::new(node_id, client_url.clone(), cluster_url.clone()), state.unix_time_ms())
    .await
```

Map `ControlResponse::Ok` to 200 JSON, `Rejected` to 400, unexpected control responses to 500, and `MetaRaftError` to 500.

- [x] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p ursula admin_register_node
```

Expected: both new tests pass.

### Task 3: Regression Checks

**Files:**
- Modify: none

- [x] **Step 1: Run focused Ursula HTTP tests**

Run:

```bash
cargo test -p ursula admin_register_node
```

Expected: both admin node registration tests pass.

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
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase7-admin-node-registration.md Cargo.lock crates/ursula/Cargo.toml crates/ursula/src/lib.rs crates/ursula/src/tests.rs
git commit -m "feat(ursula): add admin node registration route"
```
