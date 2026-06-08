# Dynamic Group Membership Phase 2 Meta Raft State Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first OpenRaft-facing meta group primitive: a pure `MetaRaftTypeConfig` and `MetaRaftStateMachine` that persists and snapshots `ursula-control::ControlPlaneState`.

**Architecture:** Keep the meta group separate from data-group Raft. Data groups continue using `UrsulaRaftTypeConfig`, protobuf `RaftGroupCommand`, and `RaftGroupStateMachine`; the meta group gets its own type config with `D = ControlCommand`, `R = ControlResponse`, and `SnapshotData = Cursor<Vec<u8>>`. This phase does not wire the meta group into bootstrap, networking, durable log storage, HTTP, CLI, or the migration executor.

**Tech Stack:** Rust 2024, OpenRaft 0.10 `declare_raft_types!`, `RaftStateMachine`, `RaftSnapshotBuilder`, `serde_json`, `ursula-control`, focused unit tests.

---

## Scope Check

The approved design still includes meta-group bootstrap, data-group OpenRaft membership operations, runtime placement routing, admin API, and CLI. This plan implements only the next testable slice: the meta Raft state machine contract. Later plans will add log-store support for `MetaRaftTypeConfig`, bootstrap the meta group, and expose admin writes through `Raft::client_write`.

This plan uses current OpenRaft documentation guidance for:

- `openraft::declare_raft_types!` with custom `D`, `R`, `Node`, `SnapshotData`, and runtime.
- `RaftStateMachine::apply`, `applied_state`, `get_snapshot_builder`, `begin_receiving_snapshot`, `install_snapshot`, and `get_current_snapshot`.
- `EntryResponder::send(response)` for every applied entry that has a responder.

## File Structure

- Modify `crates/ursula-raft/Cargo.toml`: add `ursula-control` dependency.
- Modify `crates/ursula-raft/src/lib.rs`: add and export the new meta module.
- Create `crates/ursula-raft/src/meta.rs`: meta Raft type config, state machine, snapshot builder, and tests.
- Modify `crates/ursula-control/src/command.rs`: add short `Display` names required by OpenRaft `AppData`.
- Modify `crates/ursula-control/src/tests.rs`: cover stable `Display` names.
- Modify `Cargo.lock`: record the new `ursula-raft` dependency on `ursula-control`.

No protobuf changes are required in this phase because `ControlCommand`, `ControlResponse`, and `ControlPlaneState` already derive serde. This keeps the meta group independent from the data-group protobuf command path.

---

### Task 1: Add Meta Module Skeleton

**Files:**
- Modify: `crates/ursula-raft/Cargo.toml`
- Modify: `crates/ursula-raft/src/lib.rs`
- Create: `crates/ursula-raft/src/meta.rs`
- Modify: `crates/ursula-control/src/command.rs`
- Modify: `crates/ursula-control/src/tests.rs`
- Modify: `Cargo.lock`

- [x] **Step 1: Add dependency**

In `crates/ursula-raft/Cargo.toml`, add `ursula-control` next to the other Ursula crate dependencies:

```toml
ursula-control = { workspace = true }
```

- [x] **Step 2: Export the module**

In `crates/ursula-raft/src/lib.rs`, add `meta` to the module map comment:

```rust
//! - [`meta`]: meta-group OpenRaft type config and control-plane state machine.
```

Add the module declaration near the other private modules:

```rust
mod meta;
```

Add public exports near the other `pub use` items:

```rust
pub use meta::MetaRaftSnapshotBuilder;
pub use meta::MetaRaftStateMachine;
pub use meta::MetaRaftTypeConfig;
```

- [x] **Step 3: Create a compiling skeleton**

Create `crates/ursula-raft/src/meta.rs`:

```rust
use std::io::Cursor;

use ursula_control::ControlCommand;
use ursula_control::ControlResponse;

#[cfg(madsim)]
type MetaOpenRaftRuntime = crate::sim_runtime::MadsimOpenRaftRuntime;
#[cfg(not(madsim))]
type MetaOpenRaftRuntime = openraft::impls::TokioRuntime;

openraft::declare_raft_types!(
    pub MetaRaftTypeConfig:
        D = ControlCommand,
        R = ControlResponse,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = MetaOpenRaftRuntime,
);

#[derive(Debug)]
pub struct MetaRaftStateMachine;

#[derive(Debug)]
pub struct MetaRaftSnapshotBuilder;
```

- [x] **Step 4: Add OpenRaft `AppData` `Display` support**

`ControlCommand` must implement `std::fmt::Display` to satisfy OpenRaft `AppData`. Add stable variant-name displays for `ControlCommand` and `ControlResponse`, with focused tests in `ursula-control`.

- [x] **Step 5: Verify skeleton compiles**

Run:

```bash
cargo check -p ursula-raft
```

Expected: PASS.

---

### Task 2: Add Failing Apply Tests

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`

- [x] **Step 1: Add tests that describe the state machine API**

Append this test module to `crates/ursula-raft/src/meta.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_util::stream;
    use openraft::Entry;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::RaftTypeConfig;
    use openraft::storage::RaftStateMachine;
    use openraft::vote::RaftLeaderId;
    use ursula_control::ControlCommand;
    use ursula_control::ControlResponse;

    use super::*;

    type LeaderId = <MetaRaftTypeConfig as RaftTypeConfig>::LeaderId;

    fn log_id(index: u64) -> openraft::LogId<LeaderId> {
        LogId {
            leader_id: LeaderId::new(1, 1),
            index,
        }
    }

    fn entry(index: u64, command: ControlCommand) -> Entry<MetaRaftTypeConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(command),
        }
    }

    #[tokio::test]
    async fn meta_state_machine_applies_control_commands() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((entry(
                1,
                ControlCommand::RegisterNode {
                    node_id: 5,
                    client_url: "http://node5:4491/".to_owned(),
                    cluster_url: "http://node5:4492/".to_owned(),
                    labels: BTreeMap::from([("az".to_owned(), "a".to_owned())]),
                    now_ms: 10,
                },
            ), None))]))
            .await
            .expect("apply register node");

        let node = machine.state().nodes.get(&5).expect("node registered");
        assert_eq!(node.client_url, "http://node5:4491");
        assert_eq!(node.cluster_url, "http://node5:4492");
        assert_eq!(node.labels.get("az").map(String::as_str), Some("a"));
        assert_eq!(machine.applied_log_id(), Some(log_id(1)));
    }

    #[tokio::test]
    async fn meta_state_machine_records_rejected_command_responses() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((entry(
                1,
                ControlCommand::RegisterNode {
                    node_id: 5,
                    client_url: "   ".to_owned(),
                    cluster_url: "http://node5:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                },
            ), None))]))
            .await
            .expect("apply rejected register node");

        assert!(!machine.state().nodes.contains_key(&5));
        assert_eq!(machine.last_response(), Some(&ControlResponse::Rejected {
            reason: "client_url must not be empty".to_owned(),
        }));
    }
}
```

- [x] **Step 2: Run the failing tests**

Run:

```bash
cargo test -p ursula-raft meta_state_machine_applies_control_commands
```

Expected: FAIL because `MetaRaftStateMachine::default`, `apply`, `state`, `applied_log_id`, and `last_response` are not implemented.

---

### Task 3: Implement Apply and State Accessors

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`

- [x] **Step 1: Replace the skeleton with state fields**

Replace the `MetaRaftStateMachine` skeleton with:

```rust
use std::io;

use openraft::EntryPayload;
use openraft::alias::LogIdOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use futures_util::Stream;
use futures_util::TryStreamExt;
use ursula_control::ControlPlaneState;

#[derive(Debug, Clone)]
pub struct MetaRaftStateMachine {
    state: ControlPlaneState,
    last_response: Option<ControlResponse>,
    last_applied_log_id: Option<LogIdOf<MetaRaftTypeConfig>>,
    last_membership: StoredMembershipOf<MetaRaftTypeConfig>,
    current_snapshot: Option<MetaCurrentSnapshot>,
}

#[derive(Debug, Clone)]
struct MetaCurrentSnapshot {
    meta: openraft::alias::SnapshotMetaOf<MetaRaftTypeConfig>,
    bytes: Vec<u8>,
}

impl Default for MetaRaftStateMachine {
    fn default() -> Self {
        Self {
            state: ControlPlaneState::default(),
            last_response: None,
            last_applied_log_id: None,
            last_membership: StoredMembershipOf::<MetaRaftTypeConfig>::default(),
            current_snapshot: None,
        }
    }
}

impl MetaRaftStateMachine {
    pub fn state(&self) -> &ControlPlaneState {
        &self.state
    }

    pub fn applied_log_id(&self) -> Option<LogIdOf<MetaRaftTypeConfig>> {
        self.last_applied_log_id
    }

    pub fn last_response(&self) -> Option<&ControlResponse> {
        self.last_response.as_ref()
    }
}
```

- [x] **Step 2: Implement `RaftStateMachine::apply` and `applied_state`**

Add this trait implementation:

```rust
impl RaftStateMachine<MetaRaftTypeConfig> for MetaRaftStateMachine {
    type SnapshotBuilder = MetaRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<MetaRaftTypeConfig>>,
            StoredMembershipOf<MetaRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied_log_id, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<MetaRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
        while let Some((entry, responder)) = entries.try_next().await? {
            self.last_applied_log_id = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => ControlResponse::Ok,
                EntryPayload::Normal(command) => self.state.apply(command),
                EntryPayload::Membership(membership) => {
                    self.last_membership =
                        StoredMembershipOf::<MetaRaftTypeConfig>::new(Some(entry.log_id), membership);
                    ControlResponse::Ok
                }
            };
            self.last_response = Some(response.clone());
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaRaftSnapshotBuilder {
            state: self.state.clone(),
            meta: self.snapshot_meta(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<openraft::alias::SnapshotDataOf<MetaRaftTypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::alias::SnapshotMetaOf<MetaRaftTypeConfig>,
        snapshot: openraft::alias::SnapshotDataOf<MetaRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        self.state = serde_json::from_slice(&bytes).map_err(invalid_snapshot)?;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some(MetaCurrentSnapshot {
            meta: meta.clone(),
            bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::alias::SnapshotOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self.current_snapshot.as_ref().map(|snapshot| {
            openraft::alias::SnapshotOf::<MetaRaftTypeConfig> {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.bytes.clone()),
            }
        }))
    }
}
```

- [x] **Step 3: Add snapshot helper methods**

Add these helpers outside the trait implementation:

```rust
impl MetaRaftStateMachine {
    fn snapshot_meta(&self) -> openraft::alias::SnapshotMetaOf<MetaRaftTypeConfig> {
        openraft::alias::SnapshotMetaOf::<MetaRaftTypeConfig> {
            last_log_id: self.last_applied_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: self
                .last_applied_log_id
                .map(|log_id| {
                    format!(
                        "meta-{}-{}",
                        log_id.committed_leader_id(),
                        log_id.index()
                    )
                })
                .unwrap_or_else(|| "meta-empty".to_owned()),
        }
    }
}

fn invalid_snapshot(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
```

- [x] **Step 4: Run apply tests**

Run:

```bash
cargo test -p ursula-raft meta_state_machine_applies_control_commands
cargo test -p ursula-raft meta_state_machine_records_rejected_command_responses
```

Expected: PASS for both tests.

---

### Task 4: Implement Snapshot Builder Tests and Code

**Files:**
- Modify: `crates/ursula-raft/src/meta.rs`

- [x] **Step 1: Add snapshot tests**

Append these tests inside the existing `#[cfg(test)] mod tests`:

```rust
    use openraft::storage::RaftSnapshotBuilder;

    #[tokio::test]
    async fn meta_snapshot_builder_round_trips_control_state() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((entry(
                1,
                ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                },
            ), None))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut restored = MetaRaftStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        assert!(restored.state().nodes.contains_key(&7));
        assert_eq!(restored.applied_log_id(), Some(log_id(1)));
    }

    #[tokio::test]
    async fn meta_state_machine_reports_current_snapshot_after_install() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((entry(
                1,
                ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                },
            ), None))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut restored = MetaRaftStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");
        let current = restored
            .get_current_snapshot()
            .await
            .expect("current snapshot")
            .expect("snapshot present");

        assert_eq!(current.meta.last_log_id, Some(log_id(1)));
        let decoded: ursula_control::ControlPlaneState =
            serde_json::from_slice(current.snapshot.into_inner().as_slice())
                .expect("decode current snapshot");
        assert!(decoded.nodes.contains_key(&7));
    }
```

- [x] **Step 2: Run the snapshot test**

Run:

```bash
cargo test -p ursula-raft meta_snapshot_builder_round_trips_control_state
```

Expected: FAIL because `MetaRaftSnapshotBuilder` does not implement `RaftSnapshotBuilder`.

- [x] **Step 3: Implement snapshot builder**

Replace the `MetaRaftSnapshotBuilder` skeleton with:

```rust
#[derive(Debug, Clone)]
pub struct MetaRaftSnapshotBuilder {
    state: ControlPlaneState,
    meta: openraft::alias::SnapshotMetaOf<MetaRaftTypeConfig>,
}

impl openraft::storage::RaftSnapshotBuilder<MetaRaftTypeConfig> for MetaRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::alias::SnapshotOf<MetaRaftTypeConfig>, io::Error> {
        let bytes = serde_json::to_vec(&self.state).map_err(invalid_snapshot)?;
        Ok(openraft::alias::SnapshotOf::<MetaRaftTypeConfig> {
            meta: self.meta.clone(),
            snapshot: Cursor::new(bytes),
        })
    }
}
```

- [x] **Step 4: Run snapshot tests**

Run:

```bash
cargo test -p ursula-raft meta_snapshot
cargo test -p ursula-raft meta_state_machine_reports_current_snapshot_after_install
```

Expected: PASS.

---

### Task 5: Public API and Verification

**Files:**
- Verify: `crates/ursula-raft/Cargo.toml`
- Verify: `crates/ursula-raft/src/lib.rs`
- Verify: `crates/ursula-raft/src/meta.rs`

- [x] **Step 1: Check public exports compile**

Add this test inside `crates/ursula-raft/src/meta.rs` tests:

```rust
    #[test]
    fn public_meta_types_are_exported_from_crate_root() {
        fn assert_types(
            _machine: crate::MetaRaftStateMachine,
            _builder: Option<crate::MetaRaftSnapshotBuilder>,
        ) {
        }

        assert_types(crate::MetaRaftStateMachine::default(), None);
        let _type_name = std::any::type_name::<crate::MetaRaftTypeConfig>();
    }
```

- [x] **Step 2: Run focused tests**

Run:

```bash
cargo test -p ursula-raft meta_state_machine
cargo test -p ursula-raft meta_snapshot
```

Expected: PASS.

- [x] **Step 3: Run format and lint**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Commit Phase 2**

Stage only the phase 2 files:

```bash
git add Cargo.lock crates/ursula-control/src/command.rs crates/ursula-control/src/tests.rs crates/ursula-raft/Cargo.toml crates/ursula-raft/src/lib.rs crates/ursula-raft/src/meta.rs docs/superpowers/plans/2026-06-07-dynamic-group-membership-phase2-meta-raft-state-machine.md
git commit -m "feat(ursula-raft): add meta raft state machine"
```

Expected: commit succeeds.

---

## Self-Review Notes

- Covered by this plan: a separate OpenRaft type config for meta-group commands and responses, deterministic application of `ControlCommand`, membership bookkeeping, JSON snapshots, snapshot installation, and public exports.
- Deferred to later plans: generic/dedicated durable log store for `MetaRaftTypeConfig`, meta-group bootstrap, admin API, CLI, migration executor, dynamic data-group ownership, OpenRaft `add_learner` / `change_membership`, and HTTP routing.
- The plan intentionally does not modify `crates/ursula-proto` because the meta group can persist `ControlCommand` and `ControlResponse` through OpenRaft serde, while data groups keep the existing protobuf command path.
