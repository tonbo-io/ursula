# Dynamic Group Membership Phase 4 Meta Raft Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the meta group can bootstrap as a single-node OpenRaft group and apply explicit node registration through `client_write`.

**Architecture:** Reuse Ursula's existing in-memory meta log store and meta state machine. Extend the existing single-node OpenRaft test network to the meta type config, then add an integration-style test that initializes a single-node meta group, waits for leadership, writes `RegisterNode`, and reads the state machine through OpenRaft.

**Tech Stack:** Rust, Tokio tests, OpenRaft, `ursula-control`, `ursula-raft`.

---

## File Structure

- `crates/ursula-raft/src/tests.rs`: add the OpenRaft bootstrap regression test for the meta group.
- `crates/ursula-raft/src/registry.rs`: add concrete `SingleNodeRaftNetworkFactory` / `SingleNodeRaftNetwork` trait impls for `MetaRaftTypeConfig`.

### Task 1: Meta Single-Node Bootstrap Test

**Files:**
- Modify: `crates/ursula-raft/src/tests.rs`

- [x] **Step 1: Write the failing test**

Add this test near `single_node_openraft_group_applies_client_writes`:

```rust
#[tokio::test]
async fn single_node_meta_raft_applies_node_registration() {
    let config = Arc::new(
        Config {
            cluster_name: "ursula-meta-single-node-test".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 30,
            election_timeout_max: 60,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let mut log_store = MetaRaftLogStore::shared();
    let state_machine = MetaRaftStateMachine::default();
    let raft = Raft::<MetaRaftTypeConfig, MetaRaftStateMachine>::new(
        1,
        config,
        SingleNodeRaftNetworkFactory,
        log_store.clone(),
        state_machine,
    )
    .await
    .expect("create single-node meta raft group");

    let mut nodes = BTreeMap::new();
    nodes.insert(1, BasicNode::new("meta-local"));
    raft.initialize(nodes)
        .await
        .expect("initialize single-node meta raft group");
    raft.wait(Some(Duration::from_secs(2)))
        .current_leader(1, "single-node meta group should elect itself")
        .await
        .expect("wait for meta leadership");

    let registered = raft
        .client_write(ControlCommand::RegisterNode {
            node_id: 4,
            client_url: "http://node4:4491/".to_owned(),
            cluster_url: "http://node4:4492/".to_owned(),
            labels: BTreeMap::from([("rack".to_owned(), "r1".to_owned())]),
            now_ms: 10,
        })
        .await
        .expect("register node through meta raft");
    assert_eq!(registered.data, ursula_control::ControlResponse::Ok);

    let updated = raft
        .client_write(ControlCommand::SetNodeState {
            node_id: 4,
            state: ursula_control::NodeState::Draining,
            now_ms: 20,
        })
        .await
        .expect("update registered node through meta raft");
    assert_eq!(updated.data, ursula_control::ControlResponse::Ok);

    let state = raft
        .with_state_machine(|state_machine| {
            Box::pin(async move {
                let node = state_machine
                    .state()
                    .nodes
                    .get(&4)
                    .expect("node registered through raft")
                    .clone();
                (state_machine.applied_log_id(), node)
            })
        })
        .await
        .expect("read meta state machine");
    assert_eq!(state.1.client_url, "http://node4:4491");
    assert_eq!(state.1.cluster_url, "http://node4:4492");
    assert_eq!(state.1.state, ursula_control::NodeState::Draining);
    assert_eq!(state.1.labels.get("rack").map(String::as_str), Some("r1"));
    assert!(state.0.is_some());

    let log_state = log_store.get_log_state().await.expect("meta log state");
    assert!(log_state.last_log_id.is_some());
    raft.shutdown()
        .await
        .expect("shutdown single-node meta raft group");
}
```

- [x] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p ursula-raft single_node_meta_raft_applies_node_registration
```

Expected: compile failure because `SingleNodeRaftNetworkFactory` does not yet implement `RaftNetworkFactory<MetaRaftTypeConfig>`.

### Task 2: Meta Single-Node Network Impl

**Files:**
- Modify: `crates/ursula-raft/src/registry.rs`

- [x] **Step 1: Add the concrete meta network impls**

Add `use crate::meta::MetaRaftTypeConfig;`, then add `RaftNetworkFactory<MetaRaftTypeConfig>` and `RaftNetworkV2<MetaRaftTypeConfig>` impls for the existing single-node network types. The methods mirror the data-group impls and remain unreachable because a one-node cluster never sends RPCs to another node.

- [x] **Step 2: Run test to verify it passes**

Run:

```bash
cargo test -p ursula-raft single_node_meta_raft_applies_node_registration
```

Expected: the new test passes.

### Task 3: Regression Checks

**Files:**
- Modify: none

- [x] **Step 1: Run focused meta tests**

Run:

```bash
cargo test -p ursula-raft meta
```

Expected: all meta state machine, meta log store, and new meta bootstrap tests pass.

- [x] **Step 2: Run data-group single-node regression**

Run:

```bash
cargo test -p ursula-raft single_node_openraft_group_applies_client_writes
```

Expected: existing data-group single-node bootstrap still passes.

- [x] **Step 3: Run formatting and lint checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Expected: all checks exit successfully.

- [x] **Step 4: Commit**

Run:

```bash
git add docs/superpowers/plans/2026-06-08-dynamic-group-membership-phase4-meta-raft-bootstrap.md crates/ursula-raft/src/tests.rs crates/ursula-raft/src/registry.rs
git commit -m "feat(ursula-raft): bootstrap meta raft group"
```
