# Dynamic Group Membership Phase 1 Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the pure Rust control-plane foundation for dynamic group membership: node registration, desired data-group placement, migration records, and single-migration locking.

**Architecture:** Introduce a new `ursula-control` crate with deterministic command application on `ControlPlaneState`. This phase does not wire the control state into OpenRaft, HTTP, runtime ownership, or CLI; later phases will persist these commands through the meta Raft group and consume `GroupPlacementView` for routing and hosting decisions.

**Tech Stack:** Rust 2024, Cargo workspace, `serde`, `BTreeMap`, `BTreeSet`, `ursula-shard::RaftGroupId`, unit tests.

---

## Scope Check

The approved spec spans control state, data-group OpenRaft operations, runtime ownership, HTTP/admin routing, and CLI. This plan implements only the first executable slice: a tested `ursula-control` crate. Later plans will wire it into Raft, runtime, HTTP, and CLI.

## File Structure

- Modify `Cargo.toml`: add `crates/ursula-control` as a workspace member and dependency.
- Create `crates/ursula-control/Cargo.toml`: crate manifest.
- Create `crates/ursula-control/src/lib.rs`: public exports.
- Create `crates/ursula-control/src/model.rs`: serializable domain model.
- Create `crates/ursula-control/src/view.rs`: runtime/HTTP placement projection helpers.
- Create `crates/ursula-control/src/command.rs`: deterministic state-machine commands and responses.
- Create `crates/ursula-control/src/state.rs`: `ControlPlaneState::apply` and command handlers.
- Create `crates/ursula-control/src/tests.rs`: focused unit tests for the control state machine.

---

### Task 1: Add Workspace Crate Skeleton

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/ursula-control/Cargo.toml`
- Create: `crates/ursula-control/src/lib.rs`

- [ ] **Step 1: Add the workspace member**

In root `Cargo.toml`, add the new member immediately after `crates/ursula-proto`:

```toml
    "crates/ursula-control",
```

Add the local workspace dependency near existing Ursula crates:

```toml
ursula-control = { path = "crates/ursula-control", version = "0.1.0" }
```

- [ ] **Step 2: Create the crate manifest**

Create `crates/ursula-control/Cargo.toml`:

```toml
[package]
name = "ursula-control"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true
homepage.workspace = true
readme.workspace = true
description = "Control-plane state for Ursula dynamic node registration, group placement, and manual group migration."

[dependencies]
serde = { workspace = true }
ursula-shard = { workspace = true }

[lints.clippy]
wildcard_imports = "deny"
```

- [ ] **Step 3: Create crate exports that reference missing modules**

Create `crates/ursula-control/src/lib.rs`:

```rust
//! Control-plane state for Ursula dynamic node registration, group placement,
//! and manual group migration.
//!
//! The crate is intentionally pure data plus deterministic state transitions:
//! no I/O, no async, and no wall-clock reads.

mod command;
mod model;
mod state;
mod view;

pub use command::ControlCommand;
pub use command::ControlResponse;
pub use model::ClusterNode;
pub use model::DataGroupPlacement;
pub use model::GroupMigration;
pub use model::LearnerStatus;
pub use model::MetaConfig;
pub use model::MigrationPhase;
pub use model::NodeId;
pub use model::NodeState;
pub use state::ControlPlaneState;
pub use view::GroupPlacementView;
pub use view::PlacementNode;

#[cfg(test)]
mod tests;
```

- [ ] **Step 4: Verify the skeleton fails for the expected reason**

Run:

```bash
cargo check -p ursula-control
```

Expected: FAIL with missing `command`, `model`, `state`, `view`, and `tests` modules.

---

### Task 2: Add Domain Model and View Tests First

**Files:**
- Create: `crates/ursula-control/src/model.rs`
- Create: `crates/ursula-control/src/view.rs`
- Create: `crates/ursula-control/src/tests.rs`

- [ ] **Step 1: Write failing view tests**

Create `crates/ursula-control/src/tests.rs`:

```rust
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

use crate::ClusterNode;
use crate::GroupPlacementView;
use crate::NodeState;
use crate::PlacementNode;

fn set(values: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
    values.into_iter().collect()
}

fn placement_node(node_id: u64, state: NodeState) -> PlacementNode {
    PlacementNode {
        node_id,
        client_url: format!("http://node{node_id}:4491"),
        cluster_url: format!("http://node{node_id}:4492"),
        state,
    }
}

#[test]
fn placement_view_distinguishes_hosting_from_client_traffic() {
    let view = GroupPlacementView {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2]),
        learners: set([3]),
        draining: set([2]),
        epoch: 7,
        nodes: BTreeMap::from([
            (1, placement_node(1, NodeState::Active)),
            (2, placement_node(2, NodeState::Active)),
            (3, placement_node(3, NodeState::Active)),
        ]),
    };

    assert!(view.hosts(1));
    assert!(view.hosts(3));
    assert!(!view.hosts(4));
    assert!(view.serves_client_traffic(1));
    assert!(!view.serves_client_traffic(2));
    assert!(!view.serves_client_traffic(3));
}

#[test]
fn placement_view_selects_active_non_draining_voter_for_redirect() {
    let view = GroupPlacementView {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        learners: BTreeSet::new(),
        draining: set([2]),
        epoch: 1,
        nodes: BTreeMap::from([
            (1, placement_node(1, NodeState::Active)),
            (2, placement_node(2, NodeState::Active)),
            (3, placement_node(3, NodeState::Disabled)),
        ]),
    };

    assert_eq!(
        view.active_voter_client_url(Some(1)),
        Some((1, "http://node1:4491".to_owned()))
    );
    assert_eq!(
        view.active_voter_client_url(None),
        Some((1, "http://node1:4491".to_owned()))
    );
}

#[test]
fn cluster_node_active_state_is_migration_eligible() {
    let node = ClusterNode {
        node_id: 5,
        client_url: "http://node5:4491".to_owned(),
        cluster_url: "http://node5:4492".to_owned(),
        state: NodeState::Active,
        registered_at_ms: 10,
        updated_at_ms: 10,
        labels: BTreeMap::new(),
    };

    assert!(node.state.is_migration_eligible());
    assert!(!NodeState::Draining.is_migration_eligible());
    assert!(!NodeState::Disabled.is_migration_eligible());
    assert!(!NodeState::Removed.is_migration_eligible());
}
```

- [ ] **Step 2: Run the failing view test**

Run:

```bash
cargo test -p ursula-control placement_view_distinguishes_hosting_from_client_traffic
```

Expected: FAIL with missing domain/view modules and types.

- [ ] **Step 3: Implement `model.rs`**

Create `crates/ursula-control/src/model.rs`:

```rust
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Active,
    Draining,
    Disabled,
    Removed,
}

impl NodeState {
    pub fn is_migration_eligible(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub state: NodeState,
    pub registered_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataGroupPlacement {
    pub raft_group_id: RaftGroupId,
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub draining: BTreeSet<NodeId>,
    pub epoch: u64,
    pub updated_at_ms: u64,
}

impl DataGroupPlacement {
    pub fn empty(raft_group_id: RaftGroupId) -> Self {
        Self {
            raft_group_id,
            voters: BTreeSet::new(),
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            epoch: 0,
            updated_at_ms: 0,
        }
    }

    pub fn hosts(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id) || self.learners.contains(&node_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LearnerStatus {
    Pending,
    Adding,
    CaughtUp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MigrationPhase {
    Validating,
    PreparingLocalEngines,
    AddingLearners,
    ChangingVoters,
    VerifyingMembership,
    CommittingPlacement,
    Finalizing,
    Succeeded,
    Failed,
}

impl MigrationPhase {
    pub fn is_running(self) -> bool {
        !matches!(self, Self::Succeeded | Self::Failed)
    }

    pub fn can_advance_to(self, next: Self) -> bool {
        if !self.is_running() {
            return false;
        }
        if next == Self::Failed {
            return true;
        }
        (self as u8) < (next as u8) && next != Self::Failed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigration {
    pub migration_id: u64,
    pub raft_group_id: RaftGroupId,
    pub from_voters: BTreeSet<NodeId>,
    pub target_voters: BTreeSet<NodeId>,
    pub added_nodes: BTreeSet<NodeId>,
    pub removed_voters: BTreeSet<NodeId>,
    pub retain_removed: bool,
    pub phase: MigrationPhase,
    pub per_node_learner_status: BTreeMap<NodeId, LearnerStatus>,
    pub last_error: Option<String>,
    pub retry_count: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl GroupMigration {
    pub fn is_running(&self) -> bool {
        self.phase.is_running()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaConfig {
    pub initial_meta_voters: BTreeSet<NodeId>,
    pub default_replication_factor: u32,
    pub autopilot_enabled: bool,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            initial_meta_voters: BTreeSet::new(),
            default_replication_factor: 3,
            autopilot_enabled: false,
        }
    }
}
```

- [ ] **Step 4: Implement `view.rs`**

Create `crates/ursula-control/src/view.rs`:

```rust
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::model::NodeId;
use crate::model::NodeState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementNode {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub state: NodeState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupPlacementView {
    pub raft_group_id: RaftGroupId,
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub draining: BTreeSet<NodeId>,
    pub epoch: u64,
    pub nodes: BTreeMap<NodeId, PlacementNode>,
}

impl GroupPlacementView {
    pub fn hosts(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id) || self.learners.contains(&node_id)
    }

    pub fn serves_client_traffic(&self, node_id: NodeId) -> bool {
        self.voters.contains(&node_id)
            && !self.draining.contains(&node_id)
            && self
                .nodes
                .get(&node_id)
                .is_some_and(|node| node.state == NodeState::Active)
    }

    pub fn cluster_endpoints(&self) -> BTreeMap<NodeId, String> {
        self.nodes
            .iter()
            .filter(|(node_id, _)| self.hosts(**node_id))
            .map(|(node_id, node)| (*node_id, node.cluster_url.clone()))
            .collect()
    }

    pub fn active_voter_client_url(&self, exclude: Option<NodeId>) -> Option<(NodeId, String)> {
        let mut fallback: Option<(NodeId, String)> = None;
        for node_id in &self.voters {
            if !self.serves_client_traffic(*node_id) {
                continue;
            }
            let Some(node) = self.nodes.get(node_id) else {
                continue;
            };
            let candidate = (*node_id, node.client_url.clone());
            if Some(*node_id) == exclude {
                fallback.get_or_insert(candidate);
            } else {
                return Some(candidate);
            }
        }
        fallback
    }
}
```

- [ ] **Step 5: Create temporary empty state and command modules**

Create `crates/ursula-control/src/command.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlCommand {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlResponse {}
```

Create `crates/ursula-control/src/state.rs`:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlPlaneState;
```

- [ ] **Step 6: Run view tests**

Run:

```bash
cargo test -p ursula-control placement_view
```

Expected: PASS for the two `placement_view_*` tests.

---

### Task 3: Add Commands and Node/Placement State Tests First

**Files:**
- Modify: `crates/ursula-control/src/command.rs`
- Modify: `crates/ursula-control/src/state.rs`
- Modify: `crates/ursula-control/src/tests.rs`

- [ ] **Step 1: Write failing command/state tests**

Append these tests to `crates/ursula-control/src/tests.rs`:

```rust
use crate::ControlCommand;
use crate::ControlPlaneState;
use crate::ControlResponse;

#[test]
fn register_node_command_persists_addresses_and_active_state() {
    let mut state = ControlPlaneState::default();

    let response = state.apply(ControlCommand::RegisterNode {
        node_id: 5,
        client_url: "http://node5:4491/".to_owned(),
        cluster_url: "http://node5:4492/".to_owned(),
        labels: BTreeMap::from([("az".to_owned(), "a".to_owned())]),
        now_ms: 10,
    });

    assert_eq!(response, ControlResponse::Ok);
    let node = state.nodes.get(&5).expect("node registered");
    assert_eq!(node.client_url, "http://node5:4491");
    assert_eq!(node.cluster_url, "http://node5:4492");
    assert_eq!(node.state, NodeState::Active);
    assert_eq!(node.registered_at_ms, 10);
    assert_eq!(node.updated_at_ms, 10);
    assert_eq!(node.labels.get("az").map(String::as_str), Some("a"));
}

#[test]
fn seed_placement_records_initial_voters_without_bumping_epoch() {
    let mut state = ControlPlaneState::default();

    let response = state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });

    assert_eq!(response, ControlResponse::Ok);
    let placement = state.placements.get(&RaftGroupId(1)).expect("placement");
    assert_eq!(placement.voters, set([1, 2, 3]));
    assert_eq!(placement.learners, BTreeSet::new());
    assert_eq!(placement.draining, BTreeSet::new());
    assert_eq!(placement.epoch, 0);
    assert_eq!(placement.updated_at_ms, 20);
}

#[test]
fn placement_view_from_state_includes_voters_learners_and_draining_nodes() {
    let mut state = ControlPlaneState::default();
    for node_id in 1..=3 {
        state.apply(ControlCommand::RegisterNode {
            node_id,
            client_url: format!("http://node{node_id}:4491"),
            cluster_url: format!("http://node{node_id}:4492"),
            labels: BTreeMap::new(),
            now_ms: 10,
        });
    }
    state.apply(ControlCommand::CommitPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1]),
        learners: set([2]),
        draining: set([3]),
        now_ms: 30,
    });

    let view = state.placement_view(RaftGroupId(1)).expect("view exists");

    assert_eq!(view.voters, set([1]));
    assert_eq!(view.learners, set([2]));
    assert_eq!(view.draining, set([3]));
    assert_eq!(view.nodes.len(), 3);
}
```

- [ ] **Step 2: Run the failing state test**

Run:

```bash
cargo test -p ursula-control register_node_command_persists_addresses_and_active_state
```

Expected: FAIL with missing `ControlCommand` variants, `ControlResponse::Ok`, state fields, and `ControlPlaneState::apply`.

- [ ] **Step 3: Implement commands**

Replace `crates/ursula-control/src/command.rs` with:

```rust
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::model::LearnerStatus;
use crate::model::MigrationPhase;
use crate::model::NodeId;
use crate::model::NodeState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommand {
    RegisterNode {
        node_id: NodeId,
        client_url: String,
        cluster_url: String,
        #[serde(default)]
        labels: BTreeMap<String, String>,
        now_ms: u64,
    },
    SetNodeState {
        node_id: NodeId,
        state: NodeState,
        now_ms: u64,
    },
    SeedPlacement {
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        now_ms: u64,
    },
    BeginMigration {
        raft_group_id: RaftGroupId,
        target_voters: BTreeSet<NodeId>,
        retain_removed: bool,
        now_ms: u64,
    },
    AdvanceMigration {
        migration_id: u64,
        phase: MigrationPhase,
        now_ms: u64,
    },
    SetLearnerStatus {
        migration_id: u64,
        node_id: NodeId,
        status: LearnerStatus,
        now_ms: u64,
    },
    RecordMigrationError {
        migration_id: u64,
        error: String,
        now_ms: u64,
    },
    CommitPlacement {
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
        draining: BTreeSet<NodeId>,
        now_ms: u64,
    },
    FinishMigration {
        migration_id: u64,
        success: bool,
        now_ms: u64,
    },
    EvictLearner {
        raft_group_id: RaftGroupId,
        node_id: NodeId,
        now_ms: u64,
    },
}

impl ControlCommand {
    pub fn now_ms(&self) -> u64 {
        match self {
            Self::RegisterNode { now_ms, .. }
            | Self::SetNodeState { now_ms, .. }
            | Self::SeedPlacement { now_ms, .. }
            | Self::BeginMigration { now_ms, .. }
            | Self::AdvanceMigration { now_ms, .. }
            | Self::SetLearnerStatus { now_ms, .. }
            | Self::RecordMigrationError { now_ms, .. }
            | Self::CommitPlacement { now_ms, .. }
            | Self::FinishMigration { now_ms, .. }
            | Self::EvictLearner { now_ms, .. } => *now_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok,
    MigrationStarted { migration_id: u64 },
    Rejected { reason: String },
}

impl ControlResponse {
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }
}
```

- [ ] **Step 4: Implement state registration and placement**

Replace `crates/ursula-control/src/state.rs` with:

```rust
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::command::ControlCommand;
use crate::command::ControlResponse;
use crate::model::ClusterNode;
use crate::model::DataGroupPlacement;
use crate::model::GroupMigration;
use crate::model::LearnerStatus;
use crate::model::MetaConfig;
use crate::model::MigrationPhase;
use crate::model::NodeId;
use crate::model::NodeState;
use crate::view::GroupPlacementView;
use crate::view::PlacementNode;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub nodes: BTreeMap<NodeId, ClusterNode>,
    pub placements: BTreeMap<RaftGroupId, DataGroupPlacement>,
    pub migrations: BTreeMap<u64, GroupMigration>,
    pub active_migration: Option<u64>,
    pub next_migration_id: u64,
    pub config: MetaConfig,
}

impl ControlPlaneState {
    pub fn new(config: MetaConfig) -> Self {
        Self {
            next_migration_id: 1,
            config,
            ..Self::default()
        }
    }

    pub fn apply(&mut self, command: ControlCommand) -> ControlResponse {
        match command {
            ControlCommand::RegisterNode {
                node_id,
                client_url,
                cluster_url,
                labels,
                now_ms,
            } => self.register_node(node_id, client_url, cluster_url, labels, now_ms),
            ControlCommand::SetNodeState {
                node_id,
                state,
                now_ms,
            } => match self.nodes.get_mut(&node_id) {
                Some(node) => {
                    node.state = state;
                    node.updated_at_ms = now_ms;
                    ControlResponse::Ok
                }
                None => reject(format!("node {node_id} is not registered")),
            },
            ControlCommand::SeedPlacement {
                raft_group_id,
                voters,
                now_ms,
            } => {
                if voters.is_empty() {
                    return reject("voters must not be empty".to_owned());
                }
                let placement = self
                    .placements
                    .entry(raft_group_id)
                    .or_insert_with(|| DataGroupPlacement::empty(raft_group_id));
                placement.voters = voters;
                placement.learners = BTreeSet::new();
                placement.draining = BTreeSet::new();
                placement.updated_at_ms = now_ms;
                ControlResponse::Ok
            }
            ControlCommand::CommitPlacement {
                raft_group_id,
                voters,
                learners,
                draining,
                now_ms,
            } => self.commit_placement(raft_group_id, voters, learners, draining, now_ms),
            _ => reject("command is not implemented in this phase".to_owned()),
        }
    }

    fn register_node(
        &mut self,
        node_id: NodeId,
        client_url: String,
        cluster_url: String,
        labels: BTreeMap<String, String>,
        now_ms: u64,
    ) -> ControlResponse {
        let client_url = normalize_url(client_url);
        let cluster_url = normalize_url(cluster_url);
        if client_url.is_empty() {
            return reject("client_url must not be empty".to_owned());
        }
        if cluster_url.is_empty() {
            return reject("cluster_url must not be empty".to_owned());
        }
        self.nodes
            .entry(node_id)
            .and_modify(|node| {
                node.client_url = client_url.clone();
                node.cluster_url = cluster_url.clone();
                node.labels = labels.clone();
                node.updated_at_ms = now_ms;
                if node.state == NodeState::Removed {
                    node.state = NodeState::Active;
                }
            })
            .or_insert_with(|| ClusterNode {
                node_id,
                client_url,
                cluster_url,
                state: NodeState::Active,
                registered_at_ms: now_ms,
                updated_at_ms: now_ms,
                labels,
            });
        ControlResponse::Ok
    }

    fn commit_placement(
        &mut self,
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
        draining: BTreeSet<NodeId>,
        now_ms: u64,
    ) -> ControlResponse {
        if voters.is_empty() {
            return reject("voters must not be empty".to_owned());
        }
        let placement = self
            .placements
            .entry(raft_group_id)
            .or_insert_with(|| DataGroupPlacement::empty(raft_group_id));
        let changed = placement.voters != voters;
        placement.voters = voters;
        placement.learners = learners;
        placement.draining = draining;
        placement.updated_at_ms = now_ms;
        if changed {
            placement.epoch = placement.epoch.saturating_add(1);
        }
        ControlResponse::Ok
    }

    pub fn placement_view(&self, raft_group_id: RaftGroupId) -> Option<GroupPlacementView> {
        let placement = self.placements.get(&raft_group_id)?;
        let mut nodes = BTreeMap::new();
        for node_id in placement
            .voters
            .iter()
            .chain(placement.learners.iter())
            .chain(placement.draining.iter())
        {
            if let Some(node) = self.nodes.get(node_id) {
                nodes.insert(
                    *node_id,
                    PlacementNode {
                        node_id: *node_id,
                        client_url: node.client_url.clone(),
                        cluster_url: node.cluster_url.clone(),
                        state: node.state,
                    },
                );
            }
        }
        Some(GroupPlacementView {
            raft_group_id,
            voters: placement.voters.clone(),
            learners: placement.learners.clone(),
            draining: placement.draining.clone(),
            epoch: placement.epoch,
            nodes,
        })
    }
}

fn normalize_url(value: String) -> String {
    value.trim().trim_end_matches('/').to_owned()
}

fn reject(reason: String) -> ControlResponse {
    ControlResponse::Rejected { reason }
}
```

- [ ] **Step 5: Run command/state tests**

Run:

```bash
cargo test -p ursula-control register_node_command_persists_addresses_and_active_state seed_placement_records_initial_voters_without_bumping_epoch placement_view_from_state_includes_voters_learners_and_draining_nodes
```

Expected: PASS for these three tests.

---

### Task 4: Add Migration State Tests and Implement Migration Commands

**Files:**
- Modify: `crates/ursula-control/src/state.rs`
- Modify: `crates/ursula-control/src/tests.rs`

- [ ] **Step 1: Write failing migration tests**

Append these tests to `crates/ursula-control/src/tests.rs`:

```rust
fn register_active_nodes(state: &mut ControlPlaneState, nodes: impl IntoIterator<Item = u64>) {
    for node_id in nodes {
        assert_eq!(
            state.apply(ControlCommand::RegisterNode {
                node_id,
                client_url: format!("http://node{node_id}:4491"),
                cluster_url: format!("http://node{node_id}:4492"),
                labels: BTreeMap::new(),
                now_ms: 10,
            }),
            ControlResponse::Ok
        );
    }
}

#[test]
fn begin_migration_records_added_and_removed_nodes() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=5);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });

    let response = state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 4, 5]),
        retain_removed: true,
        now_ms: 30,
    });

    assert_eq!(response, ControlResponse::MigrationStarted { migration_id: 1 });
    assert_eq!(state.active_migration, Some(1));
    let migration = state.migrations.get(&1).expect("migration");
    assert_eq!(migration.from_voters, set([1, 2, 3]));
    assert_eq!(migration.target_voters, set([2, 4, 5]));
    assert_eq!(migration.added_nodes, set([4, 5]));
    assert_eq!(migration.removed_voters, set([1, 3]));
    assert_eq!(migration.phase, crate::MigrationPhase::Validating);
    assert!(migration.retain_removed);
}

#[test]
fn migration_lock_rejects_second_running_migration() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(2),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });

    let response = state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(2),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 31,
    });

    assert_eq!(
        response,
        ControlResponse::Rejected {
            reason: "migration 1 is already running".to_owned(),
        }
    );
}

#[test]
fn finish_migration_releases_lock_and_commit_records_retained_learners() {
    let mut state = ControlPlaneState::new(crate::MetaConfig::default());
    register_active_nodes(&mut state, 1..=4);
    state.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(1),
        voters: set([1, 2, 3]),
        now_ms: 20,
    });
    state.apply(ControlCommand::BeginMigration {
        raft_group_id: RaftGroupId(1),
        target_voters: set([2, 3, 4]),
        retain_removed: true,
        now_ms: 30,
    });

    assert_eq!(
        state.apply(ControlCommand::CommitPlacement {
            raft_group_id: RaftGroupId(1),
            voters: set([2, 3, 4]),
            learners: set([1]),
            draining: set([1]),
            now_ms: 40,
        }),
        ControlResponse::Ok
    );
    assert_eq!(
        state.apply(ControlCommand::FinishMigration {
            migration_id: 1,
            success: true,
            now_ms: 41,
        }),
        ControlResponse::Ok
    );

    let placement = state.placements.get(&RaftGroupId(1)).expect("placement");
    assert_eq!(placement.voters, set([2, 3, 4]));
    assert_eq!(placement.learners, set([1]));
    assert_eq!(placement.draining, set([1]));
    assert_eq!(placement.epoch, 1);
    assert_eq!(state.active_migration, None);
    assert_eq!(
        state.migrations.get(&1).expect("migration").phase,
        crate::MigrationPhase::Succeeded
    );
}
```

- [ ] **Step 2: Run the failing migration test**

Run:

```bash
cargo test -p ursula-control begin_migration_records_added_and_removed_nodes
```

Expected: FAIL because `BeginMigration` still returns the generic "command is not implemented in this phase" rejection.

- [ ] **Step 3: Implement migration command dispatch**

In `ControlPlaneState::apply`, replace:

```rust
_ => reject("command is not implemented in this phase".to_owned()),
```

with:

```rust
ControlCommand::BeginMigration {
    raft_group_id,
    target_voters,
    retain_removed,
    now_ms,
} => self.begin_migration(raft_group_id, target_voters, retain_removed, now_ms),
ControlCommand::AdvanceMigration {
    migration_id,
    phase,
    now_ms,
} => self.advance_migration(migration_id, phase, now_ms),
ControlCommand::SetLearnerStatus {
    migration_id,
    node_id,
    status,
    now_ms,
} => self.set_learner_status(migration_id, node_id, status, now_ms),
ControlCommand::RecordMigrationError {
    migration_id,
    error,
    now_ms,
} => self.record_migration_error(migration_id, error, now_ms),
ControlCommand::FinishMigration {
    migration_id,
    success,
    now_ms,
} => self.finish_migration(migration_id, success, now_ms),
ControlCommand::EvictLearner {
    raft_group_id,
    node_id,
    now_ms,
} => self.evict_learner(raft_group_id, node_id, now_ms),
```

- [ ] **Step 4: Add migration methods**

Add these methods to `impl ControlPlaneState`:

```rust
fn begin_migration(
    &mut self,
    raft_group_id: RaftGroupId,
    target_voters: BTreeSet<NodeId>,
    retain_removed: bool,
    now_ms: u64,
) -> ControlResponse {
    if let Some(active) = self.active_migration {
        return reject(format!("migration {active} is already running"));
    }
    if target_voters.is_empty() {
        return reject("target_voters must not be empty".to_owned());
    }
    for node_id in &target_voters {
        match self.nodes.get(node_id) {
            Some(node) if node.state.is_migration_eligible() => {}
            Some(node) => {
                return reject(format!(
                    "target node {node_id} is not Active (state {:?})",
                    node.state
                ));
            }
            None => return reject(format!("target node {node_id} is not registered")),
        }
    }

    let Some(placement) = self.placements.get(&raft_group_id) else {
        return reject(format!("group {} has no placement", raft_group_id.0));
    };
    let from_voters = placement.voters.clone();
    let added_nodes = target_voters.difference(&from_voters).copied().collect();
    let removed_voters = from_voters.difference(&target_voters).copied().collect();
    let per_node_learner_status = target_voters
        .difference(&from_voters)
        .map(|node_id| (*node_id, LearnerStatus::Pending))
        .collect();

    let migration_id = self.next_migration_id.max(1);
    self.next_migration_id = migration_id.saturating_add(1);
    self.migrations.insert(
        migration_id,
        GroupMigration {
            migration_id,
            raft_group_id,
            from_voters,
            target_voters,
            added_nodes,
            removed_voters,
            retain_removed,
            phase: MigrationPhase::Validating,
            per_node_learner_status,
            last_error: None,
            retry_count: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        },
    );
    self.active_migration = Some(migration_id);
    ControlResponse::MigrationStarted { migration_id }
}

fn advance_migration(
    &mut self,
    migration_id: u64,
    phase: MigrationPhase,
    now_ms: u64,
) -> ControlResponse {
    match self.migrations.get_mut(&migration_id) {
        Some(migration) => {
            if migration.phase == phase {
                return ControlResponse::Ok;
            }
            if !migration.phase.can_advance_to(phase) {
                return reject(format!(
                    "migration {migration_id} cannot advance from {:?} to {phase:?}",
                    migration.phase
                ));
            }
            migration.phase = phase;
            migration.updated_at_ms = now_ms;
            ControlResponse::Ok
        }
        None => reject(format!("migration {migration_id} does not exist")),
    }
}

fn set_learner_status(
    &mut self,
    migration_id: u64,
    node_id: NodeId,
    status: LearnerStatus,
    now_ms: u64,
) -> ControlResponse {
    match self.migrations.get_mut(&migration_id) {
        Some(migration) => {
            migration.per_node_learner_status.insert(node_id, status);
            migration.updated_at_ms = now_ms;
            ControlResponse::Ok
        }
        None => reject(format!("migration {migration_id} does not exist")),
    }
}

fn record_migration_error(
    &mut self,
    migration_id: u64,
    error: String,
    now_ms: u64,
) -> ControlResponse {
    match self.migrations.get_mut(&migration_id) {
        Some(migration) => {
            migration.last_error = Some(error);
            migration.retry_count = migration.retry_count.saturating_add(1);
            migration.updated_at_ms = now_ms;
            ControlResponse::Ok
        }
        None => reject(format!("migration {migration_id} does not exist")),
    }
}

fn finish_migration(
    &mut self,
    migration_id: u64,
    success: bool,
    now_ms: u64,
) -> ControlResponse {
    match self.migrations.get_mut(&migration_id) {
        Some(migration) => {
            migration.phase = if success {
                MigrationPhase::Succeeded
            } else {
                MigrationPhase::Failed
            };
            migration.updated_at_ms = now_ms;
            if self.active_migration == Some(migration_id) {
                self.active_migration = None;
            }
            ControlResponse::Ok
        }
        None => reject(format!("migration {migration_id} does not exist")),
    }
}

fn evict_learner(
    &mut self,
    raft_group_id: RaftGroupId,
    node_id: NodeId,
    now_ms: u64,
) -> ControlResponse {
    match self.placements.get_mut(&raft_group_id) {
        Some(placement) => {
            if placement.voters.contains(&node_id) {
                return reject(format!(
                    "node {node_id} is a voter of group {} and cannot be evicted as a learner",
                    raft_group_id.0
                ));
            }
            placement.learners.remove(&node_id);
            placement.draining.remove(&node_id);
            placement.updated_at_ms = now_ms;
            ControlResponse::Ok
        }
        None => reject(format!("group {} has no placement", raft_group_id.0)),
    }
}
```

- [ ] **Step 5: Add active migration query**

Add this method to `impl ControlPlaneState`:

```rust
pub fn active_migration(&self) -> Option<&GroupMigration> {
    self.active_migration
        .and_then(|id| self.migrations.get(&id))
}
```

- [ ] **Step 6: Run migration tests**

Run:

```bash
cargo test -p ursula-control migration
```

Expected: PASS for migration tests.

---

### Task 5: Finish Verification and Commit

**Files:**
- Verify: `Cargo.toml`
- Verify: `crates/ursula-control/Cargo.toml`
- Verify: `crates/ursula-control/src/lib.rs`
- Verify: `crates/ursula-control/src/model.rs`
- Verify: `crates/ursula-control/src/view.rs`
- Verify: `crates/ursula-control/src/command.rs`
- Verify: `crates/ursula-control/src/state.rs`
- Verify: `crates/ursula-control/src/tests.rs`

- [ ] **Step 1: Run focused tests**

Run:

```bash
cargo test -p ursula-control
```

Expected: PASS.

- [ ] **Step 2: Run formatting**

Run:

```bash
cargo fmt --all -- --check
```

Expected: PASS. If it fails, run `cargo fmt --all`, then rerun the check.

- [ ] **Step 3: Run focused clippy**

Run:

```bash
cargo clippy -p ursula-control --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Inspect status**

Run:

```bash
git status --short --untracked-files=all
```

Expected: tracked changes are `Cargo.toml` and files under `crates/ursula-control/`; the plan file under `docs/superpowers/plans/` may also be present if it is being committed with the implementation.

- [ ] **Step 5: Commit phase 1**

GitButler is not configured in the original checkout. Use git fallback in this isolated worktree:

```bash
git add Cargo.toml crates/ursula-control docs/superpowers/plans/2026-06-07-dynamic-group-membership-phase1-control.md
git commit -m "feat(ursula-control): add control-plane state foundation"
```

Expected: commit succeeds with the phase 1 control-plane crate and plan.

---

## Self-Review Notes

- Covered by this plan: node registration, node state eligibility, placement view, migration lock, migration phase transitions, retained learner placement, and learner eviction state.
- Deferred to later plans: meta Raft persistence, OpenRaft `add_learner`/`change_membership`, runtime `unload_group`, HTTP/admin routes, and CLI.
- Each production behavior has a preceding failing test step.
