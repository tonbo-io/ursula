use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::cluster::ClusterBootstrap;
use crate::cluster::ClusterBootstrapRecord;
use crate::cluster::NodeRegistration;
use crate::cluster::VerifiedGroupMembership;
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
use crate::policy::GroupPlacementPolicy;
use crate::policy::ManagedPlacement;
use crate::policy::PlacementPolicy;
use crate::policy::ReplicationFactor;
use crate::view::GroupPlacementView;
use crate::view::PlacementNode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneState {
    pub nodes: BTreeMap<NodeId, ClusterNode>,
    pub placements: BTreeMap<RaftGroupId, DataGroupPlacement>,
    pub migrations: BTreeMap<u64, GroupMigration>,
    pub active_migration: Option<u64>,
    pub next_migration_id: u64,
    pub config: MetaConfig,
    /// Absent in legacy/static control snapshots. Enabling managed mode is an
    /// explicit, validated adoption rather than a changed TOML default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_placement: Option<ManagedPlacement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_bootstrap: Option<ClusterBootstrapRecord>,
}

impl Default for ControlPlaneState {
    fn default() -> Self {
        Self::new(MetaConfig::default())
    }
}

impl ControlPlaneState {
    pub fn new(config: MetaConfig) -> Self {
        Self {
            nodes: BTreeMap::new(),
            placements: BTreeMap::new(),
            migrations: BTreeMap::new(),
            active_migration: None,
            next_migration_id: 1,
            config,
            managed_placement: None,
            cluster_bootstrap: None,
        }
    }

    pub fn apply(&mut self, command: ControlCommand) -> ControlResponse {
        match command {
            ControlCommand::BootstrapCluster {
                bootstrap,
                memberships,
                now_ms,
            } => self.bootstrap_cluster(bootstrap, memberships, now_ms),
            ControlCommand::RegisterManagedNode { node, now_ms } => {
                self.register_managed_node(node, now_ms)
            }
            ControlCommand::RegisterNode {
                node_id,
                client_url,
                cluster_url,
                labels,
                now_ms,
            } => {
                if self.cluster_bootstrap.is_some() {
                    reject("bootstrapped clusters require RegisterManagedNode with a trusted admin endpoint".to_owned())
                } else {
                    self.register_node(node_id, client_url, cluster_url, None, labels, now_ms)
                }
            }
            ControlCommand::SetNodeState {
                node_id,
                state,
                now_ms,
            } => self.set_node_state(node_id, state, now_ms),
            ControlCommand::SeedPlacement {
                raft_group_id,
                voters,
                now_ms,
            } => self.seed_placement(raft_group_id, voters, now_ms),
            ControlCommand::AdoptPlacementPolicy {
                policy,
                group_count,
                now_ms: _,
            } => self.adopt_placement_policy(policy, group_count),
            ControlCommand::CommitPlacement {
                raft_group_id,
                voters,
                learners,
                draining,
                now_ms,
            } => self.commit_placement(raft_group_id, voters, learners, draining, now_ms),
            ControlCommand::BeginMigration {
                raft_group_id,
                target_voters,
                retain_removed,
                now_ms,
            } => self.begin_migration(raft_group_id, target_voters, None, retain_removed, now_ms),
            ControlCommand::BeginPolicyMigration {
                raft_group_id,
                target_policy,
                target_voters,
                retain_removed,
                now_ms,
            } => self.begin_migration(
                raft_group_id,
                target_voters,
                Some(target_policy),
                retain_removed,
                now_ms,
            ),
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
        }
    }

    pub fn active_migration(&self) -> Option<&GroupMigration> {
        self.active_migration
            .and_then(|id| self.migrations.get(&id))
    }

    pub fn placement_view(&self, raft_group_id: RaftGroupId) -> Option<GroupPlacementView> {
        let placement = self.placements.get(&raft_group_id)?;
        let node_ids = placement
            .voters
            .iter()
            .chain(placement.learners.iter())
            .chain(placement.draining.iter());
        let nodes = node_ids
            .filter_map(|node_id| {
                self.nodes.get(node_id).map(|node| {
                    (*node_id, PlacementNode {
                        node_id: *node_id,
                        client_url: node.client_url.clone(),
                        cluster_url: node.cluster_url.clone(),
                        admin_url: node.admin_url.clone(),
                        state: node.state,
                    })
                })
            })
            .collect();

        Some(GroupPlacementView {
            raft_group_id,
            voters: placement.voters.clone(),
            learners: placement.learners.clone(),
            draining: placement.draining.clone(),
            epoch: placement.epoch,
            nodes,
            policy: self
                .managed_placement
                .as_ref()
                .and_then(|managed| managed.groups.get(&raft_group_id))
                .cloned(),
        })
    }

    fn register_node(
        &mut self,
        node_id: NodeId,
        client_url: String,
        cluster_url: String,
        admin_url: Option<String>,
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
        if self.managed_placement.is_some() {
            if node_id == 0 {
                return reject("managed node_id must be non-zero".to_owned());
            }
            if let Some(existing) = self.nodes.get(&node_id) {
                if existing.state == NodeState::Removed {
                    return reject(format!("removed node {node_id} cannot be reused"));
                }
                if existing.client_url != client_url
                    || existing.cluster_url != cluster_url
                    || existing.labels != labels
                    || existing.admin_url != admin_url
                {
                    return reject(format!(
                        "managed node {node_id} endpoints and labels are immutable"
                    ));
                }
            }
        }

        let (registered_at_ms, state) =
            self.nodes
                .get(&node_id)
                .map_or((now_ms, NodeState::Active), |node| {
                    let state = if node.state == NodeState::Removed {
                        NodeState::Active
                    } else {
                        node.state
                    };
                    (node.registered_at_ms, state)
                });
        self.nodes.insert(node_id, ClusterNode {
            node_id,
            client_url,
            cluster_url,
            admin_url,
            state,
            registered_at_ms,
            updated_at_ms: now_ms,
            labels,
        });
        ControlResponse::Ok
    }

    fn register_managed_node(&mut self, node: NodeRegistration, now_ms: u64) -> ControlResponse {
        if self.cluster_bootstrap.is_none() {
            return reject("managed node registration requires cluster bootstrap".to_owned());
        }
        let node = match node.normalize() {
            Ok(node) => node,
            Err(reason) => return reject(reason),
        };
        for existing in self
            .nodes
            .values()
            .filter(|existing| existing.node_id != node.node_id)
        {
            for origin in [&node.client_url, &node.cluster_url, &node.admin_url] {
                if origin == &existing.client_url
                    || origin == &existing.cluster_url
                    || existing.admin_url.as_ref() == Some(origin)
                {
                    return reject(format!(
                        "endpoint {origin} is already owned by node {}",
                        existing.node_id
                    ));
                }
            }
        }
        self.register_node(
            node.node_id,
            node.client_url,
            node.cluster_url,
            Some(node.admin_url),
            node.labels,
            now_ms,
        )
    }

    fn bootstrap_cluster(
        &mut self,
        bootstrap: ClusterBootstrap,
        memberships: BTreeMap<RaftGroupId, VerifiedGroupMembership>,
        now_ms: u64,
    ) -> ControlResponse {
        let bootstrap = match bootstrap.normalize() {
            Ok(bootstrap) => bootstrap,
            Err(reason) => return reject(reason),
        };
        if let Some(existing) = &self.cluster_bootstrap {
            return if existing.recipe == bootstrap {
                ControlResponse::Ok
            } else {
                reject("cluster bootstrap differs from its immutable persisted recipe".to_owned())
            };
        }
        if !self.nodes.is_empty()
            || !self.placements.is_empty()
            || !self.migrations.is_empty()
            || self.active_migration.is_some()
            || self.managed_placement.is_some()
            || self.next_migration_id != 1
        {
            return reject("cluster bootstrap requires an empty control state; existing control state needs explicit adoption".to_owned());
        }
        let meta_rf = match u32::try_from(bootstrap.initial_meta_voters.len())
            .ok()
            .and_then(|count| ReplicationFactor::try_from(count).ok())
        {
            Some(rf) => rf,
            None => {
                return reject(
                    "initial meta voter count must be independently configured as 3 or 5"
                        .to_owned(),
                );
            }
        };
        let mut candidate = Self::new(MetaConfig {
            initial_meta_voters: bootstrap.initial_meta_voters.clone(),
            ..self.config.clone()
        });
        for node in bootstrap.nodes.values() {
            candidate.nodes.insert(node.node_id, ClusterNode {
                node_id: node.node_id,
                client_url: node.client_url.clone(),
                cluster_url: node.cluster_url.clone(),
                admin_url: Some(node.admin_url.clone()),
                state: NodeState::Active,
                registered_at_ms: now_ms,
                updated_at_ms: now_ms,
                labels: node.labels.clone(),
            });
        }
        let meta_policy = GroupPlacementPolicy {
            replication_factor: meta_rf,
            failure_domain: bootstrap.placement.failure_domain.clone(),
            survive_failure_domains: bootstrap.placement.survive_failure_domains,
        };
        if let Err(reason) =
            meta_policy.validate_voters(&bootstrap.initial_meta_voters, &candidate.nodes)
        {
            return reject(format!(
                "meta bootstrap placement violates policy: {reason}"
            ));
        }
        if memberships.len() != bootstrap.identity.group_count as usize
            || bootstrap.voters.len() != memberships.len()
        {
            return reject(
                "bootstrap requires verified memberships for every configured group".to_owned(),
            );
        }
        for (group, expected) in &bootstrap.voters {
            let Some(observed) = memberships.get(group) else {
                return reject(format!("group {} has no verified membership", group.0));
            };
            if observed.voters != *expected
                || !observed.learners.is_empty()
                || observed.log_id.node_id == 0
            {
                return reject(format!(
                    "group {} verified membership differs from settled bootstrap voters",
                    group.0
                ));
            }
            let response = candidate.seed_placement(*group, observed.voters.clone(), now_ms);
            if response.is_rejected() {
                return response;
            }
        }
        let response = candidate
            .adopt_placement_policy(bootstrap.placement.clone(), bootstrap.identity.group_count);
        if response.is_rejected() {
            return response;
        }
        candidate.cluster_bootstrap = Some(ClusterBootstrapRecord {
            recipe: bootstrap,
            memberships,
        });
        *self = candidate;
        ControlResponse::Ok
    }

    fn set_node_state(
        &mut self,
        node_id: NodeId,
        state: NodeState,
        now_ms: u64,
    ) -> ControlResponse {
        if self.managed_placement.is_some() {
            if self
                .nodes
                .get(&node_id)
                .is_some_and(|node| node.state == NodeState::Removed)
                && state != NodeState::Removed
            {
                return reject(format!("removed node {node_id} cannot be reused"));
            }
            if state == NodeState::Removed
                && (self.config.initial_meta_voters.contains(&node_id)
                    || self.placements.values().any(|placement| {
                        placement.hosts(node_id) || placement.draining.contains(&node_id)
                    })
                    || self.active_migration().is_some_and(|migration| {
                        migration.from_voters.contains(&node_id)
                            || migration.target_voters.contains(&node_id)
                    }))
            {
                return reject(format!("node {node_id} still has data or meta assignments"));
            }
        }
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return reject(format!("node {node_id} is not registered"));
        };
        node.state = state;
        node.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn seed_placement(
        &mut self,
        raft_group_id: RaftGroupId,
        voters: BTreeSet<NodeId>,
        now_ms: u64,
    ) -> ControlResponse {
        if self.managed_placement.is_some() {
            return reject("managed placement cannot be reseeded".to_owned());
        }
        if voters.is_empty() {
            return reject("placement voters must not be empty".to_owned());
        }

        self.placements.insert(raft_group_id, DataGroupPlacement {
            raft_group_id,
            voters,
            learners: BTreeSet::new(),
            draining: BTreeSet::new(),
            epoch: 0,
            updated_at_ms: now_ms,
        });
        ControlResponse::Ok
    }

    fn adopt_placement_policy(
        &mut self,
        mut policy: PlacementPolicy,
        group_count: u32,
    ) -> ControlResponse {
        if let Err(reason) = policy.validate(group_count) {
            return reject(reason);
        }
        policy
            .group_overrides
            .sort_by_key(|entry| entry.raft_group_id);
        if let Some(managed) = &self.managed_placement {
            return if managed.group_count == group_count && managed.bootstrap_policy == policy {
                ControlResponse::Ok
            } else {
                reject(
                    "managed bootstrap policy/group_count differs from persisted configuration"
                        .to_owned(),
                )
            };
        }
        if self.active_migration.is_some() {
            return reject("cannot adopt placement policy during a migration".to_owned());
        }
        if self.nodes.contains_key(&0) {
            return reject("managed node_id must be non-zero".to_owned());
        }
        if self.placements.len() != group_count as usize {
            return reject(
                "adoption requires every configured group's existing placement".to_owned(),
            );
        }
        let mut groups = BTreeMap::new();
        for id in 0..group_count {
            let id = RaftGroupId(id);
            let Some(placement) = self.placements.get(&id) else {
                return reject(format!("group {} has no existing placement", id.0));
            };
            if placement.raft_group_id != id
                || !placement.learners.is_empty()
                || !placement.draining.is_empty()
            {
                return reject(format!(
                    "group {} needs a settled, uniform placement before adoption",
                    id.0
                ));
            }
            let resolved = policy.resolve(id);
            if let Err(reason) = resolved.validate_voters(&placement.voters, &self.nodes) {
                return reject(format!(
                    "group {} adoption would change or violate policy: {reason}",
                    id.0
                ));
            }
            if placement.voters.contains(&0)
                || placement.voters.iter().any(|id| {
                    self.nodes
                        .get(id)
                        .is_some_and(|node| node.state == NodeState::Removed)
                })
            {
                return reject(format!(
                    "group {} includes an invalid or removed voter",
                    id.0
                ));
            }
            groups.insert(id, resolved);
        }
        self.config.default_replication_factor = policy.default_replication_factor.into();
        self.managed_placement = Some(ManagedPlacement {
            group_count,
            bootstrap_policy: policy,
            groups,
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
            return reject("placement voters must not be empty".to_owned());
        }
        if let Some(response) = self.validate_placement_nodes(&voters, &learners, &draining) {
            return response;
        }

        let target_policy = if self.managed_placement.is_some() {
            let Some(migration) = self.active_migration() else {
                return reject("managed placement commit requires an active migration".to_owned());
            };
            if migration.raft_group_id != raft_group_id
                || migration.target_voters != voters
                || migration.phase != MigrationPhase::CommittingPlacement
            {
                return reject(
                    "managed placement commit does not match the active intent/phase".to_owned(),
                );
            }
            let Some(policy) = &migration.target_policy else {
                return reject("managed migration has no target policy".to_owned());
            };
            if let Err(reason) = policy.validate_voters(&voters, &self.nodes) {
                return reject(reason);
            }
            if !draining.is_subset(&migration.removed_voters)
                || !learners.is_subset(&migration.removed_voters)
                || (!migration.retain_removed && !learners.is_empty())
            {
                return reject(
                    "managed placement includes unauthorized learners/draining nodes".to_owned(),
                );
            }
            Some(policy.clone())
        } else {
            None
        };

        let policy_changed = target_policy.as_ref().is_some_and(|policy| {
            self.managed_placement
                .as_ref()
                .and_then(|managed| managed.groups.get(&raft_group_id))
                != Some(policy)
        });
        let placement = self
            .placements
            .entry(raft_group_id)
            .or_insert_with(|| DataGroupPlacement::empty(raft_group_id));
        if placement.voters != voters || policy_changed {
            placement.epoch = placement.epoch.saturating_add(1);
        }
        placement.voters = voters;
        placement.learners = learners;
        placement.draining = draining;
        placement.updated_at_ms = now_ms;
        if let (Some(managed), Some(policy)) = (&mut self.managed_placement, target_policy) {
            managed.groups.insert(raft_group_id, policy);
        }
        ControlResponse::Ok
    }

    fn validate_placement_nodes(
        &self,
        voters: &BTreeSet<NodeId>,
        learners: &BTreeSet<NodeId>,
        draining: &BTreeSet<NodeId>,
    ) -> Option<ControlResponse> {
        if let Some(node_id) = voters.intersection(learners).next() {
            return Some(reject(format!(
                "node {node_id} cannot be both voter and learner"
            )));
        }
        if let Some(response) =
            self.validate_registered_nodes("voter", voters, self.managed_placement.is_none())
        {
            return Some(response);
        }
        if self.managed_placement.is_some() {
            for id in voters {
                let node = self.nodes.get(id)?;
                let retained = self
                    .active_migration()
                    .is_some_and(|migration| migration.from_voters.contains(id));
                if node.state != NodeState::Active
                    && !(retained && node.state == NodeState::Draining)
                {
                    return Some(reject(format!(
                        "voter node {id} is not eligible for this placement"
                    )));
                }
            }
        }
        if let Some(response) = self.validate_registered_nodes("learner", learners, false) {
            return Some(response);
        }
        self.validate_registered_nodes("draining", draining, false)
    }

    fn validate_registered_nodes(
        &self,
        role: &str,
        node_ids: &BTreeSet<NodeId>,
        require_migration_eligible: bool,
    ) -> Option<ControlResponse> {
        for node_id in node_ids {
            let Some(node) = self.nodes.get(node_id) else {
                return Some(reject(format!("{role} node {node_id} is not registered")));
            };
            if require_migration_eligible && !node.state.is_migration_eligible() {
                return Some(reject(format!(
                    "{role} node {node_id} is not migration eligible: {:?}",
                    node.state
                )));
            }
        }
        None
    }

    fn begin_migration(
        &mut self,
        raft_group_id: RaftGroupId,
        target_voters: BTreeSet<NodeId>,
        requested_policy: Option<GroupPlacementPolicy>,
        retain_removed: bool,
        now_ms: u64,
    ) -> ControlResponse {
        if let Some(active) = self.active_migration {
            return reject(format!("migration {active} is already running"));
        }
        if target_voters.is_empty() {
            return reject("target voters must not be empty".to_owned());
        }
        let Some(placement) = self.placements.get(&raft_group_id) else {
            return reject(format!("group {} has no placement", raft_group_id.0));
        };
        let from_policy = self
            .managed_placement
            .as_ref()
            .and_then(|managed| managed.groups.get(&raft_group_id))
            .cloned();
        if self.managed_placement.is_some() && from_policy.is_none() {
            return reject("managed group has no persisted policy".to_owned());
        }
        if requested_policy.is_some() && from_policy.is_none() {
            return reject("explicit policy migration requires managed placement".to_owned());
        }
        let target_policy = requested_policy.or_else(|| from_policy.clone());
        if let Some(policy) = &from_policy
            && let Err(reason) = policy.validate_voters(&placement.voters, &self.nodes)
        {
            return reject(format!(
                "source placement violates its persisted policy: {reason}"
            ));
        }
        if let Some(policy) = &target_policy
            && let Err(reason) = policy.validate_voters(&target_voters, &self.nodes)
        {
            return reject(format!("target placement violates policy: {reason}"));
        }
        for node_id in &target_voters {
            let Some(node) = self.nodes.get(node_id) else {
                return reject(format!("node {node_id} is not registered"));
            };
            let retained_draining = from_policy.is_some()
                && placement.voters.contains(node_id)
                && node.state == NodeState::Draining;
            if !node.state.is_migration_eligible() && !retained_draining {
                return reject(format!(
                    "node {node_id} is not migration eligible: {:?}",
                    node.state
                ));
            }
        }

        let from_voters = placement.voters.clone();
        let added_nodes = target_voters
            .difference(&from_voters)
            .copied()
            .collect::<BTreeSet<_>>();
        let removed_voters = from_voters
            .difference(&target_voters)
            .copied()
            .collect::<BTreeSet<_>>();
        let per_node_learner_status = added_nodes
            .iter()
            .copied()
            .map(|node_id| (node_id, LearnerStatus::Pending))
            .collect();

        let migration_id = self.next_migration_id.max(1);
        self.next_migration_id = migration_id.saturating_add(1);
        self.migrations.insert(migration_id, GroupMigration {
            migration_id,
            raft_group_id,
            from_voters,
            target_voters,
            from_policy,
            target_policy,
            added_nodes,
            removed_voters,
            retain_removed,
            phase: MigrationPhase::Validating,
            per_node_learner_status,
            last_error: None,
            retry_count: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        });
        self.active_migration = Some(migration_id);

        ControlResponse::MigrationStarted { migration_id }
    }

    fn advance_migration(
        &mut self,
        migration_id: u64,
        phase: MigrationPhase,
        now_ms: u64,
    ) -> ControlResponse {
        if !phase.is_running() {
            return reject(format!(
                "migration {migration_id} must finish through FinishMigration"
            ));
        }
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        if migration.phase == phase {
            return ControlResponse::Ok;
        }
        if !migration.phase.can_advance_to(phase) {
            return reject(format!(
                "migration {migration_id} cannot advance from {:?} to {:?}",
                migration.phase, phase
            ));
        }
        migration.phase = phase;
        migration.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn set_learner_status(
        &mut self,
        migration_id: u64,
        node_id: NodeId,
        status: LearnerStatus,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        if !migration.is_running() {
            return reject(format!("migration {migration_id} is not running"));
        }
        if !migration.added_nodes.contains(&node_id) {
            return reject(format!(
                "node {node_id} is not an added learner for migration {migration_id}"
            ));
        }
        migration.per_node_learner_status.insert(node_id, status);
        migration.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn record_migration_error(
        &mut self,
        migration_id: u64,
        error: String,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        migration.last_error = Some(error);
        migration.retry_count = migration.retry_count.saturating_add(1);
        migration.updated_at_ms = now_ms;
        ControlResponse::Ok
    }

    fn finish_migration(
        &mut self,
        migration_id: u64,
        success: bool,
        now_ms: u64,
    ) -> ControlResponse {
        if self.managed_placement.is_some() {
            let Some(migration) = self.migrations.get(&migration_id) else {
                return reject(format!("migration {migration_id} does not exist"));
            };
            if success {
                let placement = self.placements.get(&migration.raft_group_id);
                let policy = self
                    .managed_placement
                    .as_ref()
                    .and_then(|managed| managed.groups.get(&migration.raft_group_id));
                if migration.phase != MigrationPhase::Finalizing
                    || !placement
                        .is_some_and(|placement| placement.voters == migration.target_voters)
                    || policy != migration.target_policy.as_ref()
                {
                    return reject("managed migration cannot succeed before publishing its target placement/policy".to_owned());
                }
            } else if migration.phase >= MigrationPhase::PreparingLocalEngines {
                return reject("managed migration with possible side effects requires reconciliation before unlocking".to_owned());
            }
        }
        let Some(migration) = self.migrations.get_mut(&migration_id) else {
            return reject(format!("migration {migration_id} does not exist"));
        };
        if self.active_migration != Some(migration_id) {
            return reject(format!("migration {migration_id} is not active"));
        }
        if !migration.is_running() {
            return reject(format!("migration {migration_id} is not running"));
        }
        migration.phase = if success {
            MigrationPhase::Succeeded
        } else {
            MigrationPhase::Failed
        };
        migration.updated_at_ms = now_ms;
        self.active_migration = None;
        ControlResponse::Ok
    }

    fn evict_learner(
        &mut self,
        raft_group_id: RaftGroupId,
        node_id: NodeId,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(placement) = self.placements.get_mut(&raft_group_id) else {
            return reject(format!("group {} has no placement", raft_group_id.0));
        };
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
}

fn normalize_url(value: String) -> String {
    value.trim().trim_end_matches('/').to_owned()
}

fn reject(reason: String) -> ControlResponse {
    ControlResponse::Rejected { reason }
}
