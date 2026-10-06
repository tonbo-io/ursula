//! Deterministic managed intent and evidence protocol. Evidence is supplied by
//! the trusted executor after real receiver/Raft barriers; shape validation is
//! not a substitute for those I/O barriers or receiving-process admission.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::ControlPlaneState;
use crate::ControlResponse;
use crate::GroupMigration;
use crate::GroupPlacementPolicy;
use crate::LearnerStatus;
use crate::MembershipLogId;
use crate::MigrationPhase;
use crate::NodeState;
use crate::VerifiedGroupMembership;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiverProcess {
    pub node_id: u64,
    pub incarnation: ProcessIncarnation,
}

/// Immutable request. Retrying the same key and payload returns the same ID,
/// including after completion; reusing the key for another payload is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRequest {
    pub operation_key: String,
    pub raft_group_id: RaftGroupId,
    pub expected_epoch: u64,
    pub source_membership: VerifiedGroupMembership,
    pub target_voters: BTreeSet<u64>,
    /// None preserves the resolved policy; RF changes require an explicit value.
    pub target_policy: Option<GroupPlacementPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationToken {
    pub migration_id: u64,
    pub generation: u64,
    pub executor: ReceiverProcess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorAssignment {
    pub token: MigrationToken,
    pub claim_key: ProcessIncarnation,
    pub claimed_from_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaAppliedEvidence {
    pub process: ReceiverProcess,
    pub applied_log_id: MembershipLogId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalMembershipEvidence {
    pub membership: VerifiedGroupMembership,
    pub committed_prefix: MembershipLogId,
    pub replicas: BTreeMap<u64, ReplicaAppliedEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRetirementEvidence {
    pub process: ReceiverProcess,
    pub placement_epoch: u64,
    pub membership_log_id: MembershipLogId,
    pub work_drained: bool,
    pub snapshot_references_retired: bool,
    pub local_records_reclaimed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationUpdate {
    /// Durable before dispatching activation. A lost activation reply cannot
    /// leave an apparently side-effect-free intent eligible for cancellation.
    AuthorizeReceivers,
    CertifyReceivers {
        processes: BTreeMap<u64, ProcessIncarnation>,
    },
    RecordPrepared {
        process: ReceiverProcess,
    },
    CapturePrefix {
        prefix: MembershipLogId,
    },
    RecordLearner {
        evidence: ReplicaAppliedEvidence,
    },
    /// Durable before submitting any voter change. From here failures can only
    /// roll forward/reconcile; they cannot clear the global operation lock.
    AuthorizeMembership,
    VerifyMembership {
        evidence: FinalMembershipEvidence,
    },
    PublishPlacement,
    RecordReleased {
        evidence: ReplicaRetirementEvidence,
    },
    RetireReceivers {
        processes: BTreeMap<u64, ProcessIncarnation>,
    },
    Finish,
    Cancel,
    RecordError {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedMigration {
    pub request: MigrationRequest,
    pub executor: Option<ExecutorAssignment>,
    pub revision: u64,
    pub last_update: Option<(u64, MigrationUpdate)>,
    pub receiver_activation_authorized: bool,
    pub receivers: BTreeMap<u64, ProcessIncarnation>,
    pub prepared: BTreeSet<u64>,
    pub catchup_prefix: Option<MembershipLogId>,
    pub learner_applied: BTreeMap<u64, ReplicaAppliedEvidence>,
    pub membership_may_have_changed: bool,
    pub final_membership: Option<FinalMembershipEvidence>,
    pub verification_generation: Option<u64>,
    pub published_epoch: Option<u64>,
    pub released: BTreeSet<u64>,
    pub receivers_retired: bool,
}

impl ManagedMigration {
    fn new(request: MigrationRequest) -> Self {
        Self {
            request,
            executor: None,
            revision: 0,
            last_update: None,
            receiver_activation_authorized: false,
            receivers: BTreeMap::new(),
            prepared: BTreeSet::new(),
            catchup_prefix: None,
            learner_applied: BTreeMap::new(),
            membership_may_have_changed: false,
            final_membership: None,
            verification_generation: None,
            published_epoch: None,
            released: BTreeSet::new(),
            receivers_retired: false,
        }
    }
}

fn reject(reason: impl Into<String>) -> ControlResponse {
    ControlResponse::Rejected {
        reason: reason.into(),
    }
}

/// An index alone cannot certify a different log at the same position.
fn covers(applied: &MembershipLogId, prefix: &MembershipLogId) -> bool {
    applied.node_id != 0
        && prefix.node_id != 0
        && (applied == prefix || (applied.index > prefix.index && applied.term >= prefix.term))
}

fn participants(migration: &GroupMigration) -> BTreeSet<u64> {
    migration
        .from_voters
        .union(&migration.target_voters)
        .copied()
        .collect()
}

fn process_matches(managed: &ManagedMigration, process: &ReceiverProcess) -> bool {
    managed.receivers.get(&process.node_id) == Some(&process.incarnation)
}

impl ControlPlaneState {
    /// Validate recovered/projected authority before a runtime consumes it.
    pub fn validate_migration_state(&self) -> Result<(), String> {
        if self.cluster_bootstrap.is_none() {
            return Ok(());
        }
        if self.next_executor_generation == 0 || self.next_migration_id == 0 {
            return Err("migration identity counters must be nonzero".to_owned());
        }
        let mut keys = BTreeSet::new();
        let mut running = None;
        for (id, migration) in &self.migrations {
            let managed = migration
                .managed
                .as_ref()
                .ok_or("bootstrapped migration lacks intent authority")?;
            if *id != migration.migration_id
                || *id >= self.next_migration_id
                || !keys.insert(&managed.request.operation_key)
                || managed.request.raft_group_id != migration.raft_group_id
                || managed.request.target_voters != migration.target_voters
                || managed.request.source_membership.voters != migration.from_voters
                || !managed.request.source_membership.learners.is_empty()
                || !managed.released.is_subset(&migration.removed_voters)
                || !managed.prepared.is_subset(&migration.added_nodes)
            {
                return Err("invalid recovered migration intent/identity".to_owned());
            }
            if migration.is_running() && running.replace(*id).is_some() {
                return Err("multiple active migrations".to_owned());
            }
            let generation = managed
                .executor
                .as_ref()
                .map(|assignment| assignment.token.generation);
            if let Some(assignment) = &managed.executor
                && (assignment.token.migration_id != *id
                    || assignment.token.generation == 0
                    || assignment.token.generation >= self.next_executor_generation
                    || !self.nodes.contains_key(&assignment.token.executor.node_id))
            {
                return Err("invalid recovered executor generation".to_owned());
            }
            if managed.verification_generation.is_some()
                && managed.verification_generation != generation
            {
                return Err("membership verification belongs to a different executor".to_owned());
            }
            let peers = participants(migration);
            if (!managed.receivers.is_empty()
                && managed.receivers.keys().copied().collect::<BTreeSet<_>>() != peers)
                || (!managed.receiver_activation_authorized && !managed.receivers.is_empty())
                || (managed.membership_may_have_changed
                    && (!managed.receiver_activation_authorized
                        || managed.catchup_prefix.is_none()))
            {
                return Err("invalid recovered receiver/membership authority".to_owned());
            }
            if let Some(epoch) = managed.published_epoch
                && (managed.request.expected_epoch.checked_add(1) != Some(epoch)
                    || managed.final_membership.is_none()
                    || !matches!(
                        migration.phase,
                        MigrationPhase::Finalizing | MigrationPhase::Succeeded
                    ))
            {
                return Err("invalid recovered placement publication".to_owned());
            }
            if managed.receivers_retired
                && (managed.published_epoch.is_none()
                    || managed.released != migration.removed_voters
                    || managed.verification_generation != generation
                    || generation.is_none()
                    || managed.receivers.keys().copied().collect::<BTreeSet<_>>() != peers)
            {
                return Err("invalid recovered receiver retirement".to_owned());
            }
            if migration.phase == MigrationPhase::Succeeded && !managed.receivers_retired
                || migration.phase == MigrationPhase::Failed
                    && managed.receiver_activation_authorized
            {
                return Err("invalid recovered terminal migration".to_owned());
            }
        }
        if running != self.active_migration {
            return Err("active migration index differs from its intent".to_owned());
        }
        Ok(())
    }

    pub(crate) fn submit_managed_migration(
        &mut self,
        request: MigrationRequest,
        now_ms: u64,
    ) -> ControlResponse {
        if self.cluster_bootstrap.is_none() {
            return reject("managed migration requires trusted cluster bootstrap");
        }
        if request.operation_key.is_empty()
            || request.operation_key.len() > 128
            || !request
                .operation_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
        {
            return reject("operation key must contain 1..128 ASCII identifier characters");
        }
        for migration in self.migrations.values() {
            if let Some(managed) = &migration.managed
                && managed.request.operation_key == request.operation_key
            {
                return if managed.request == request {
                    ControlResponse::MigrationStarted {
                        migration_id: migration.migration_id,
                    }
                } else {
                    reject("operation key already identifies a different immutable request")
                };
            }
        }
        let Some(placement) = self.placements.get(&request.raft_group_id) else {
            return reject("group has no placement");
        };
        if request.expected_epoch != placement.epoch || placement.epoch == u64::MAX {
            return reject("placement epoch CAS failed or exhausted");
        }
        if !placement.learners.is_empty()
            || !placement.draining.is_empty()
            || request.source_membership.voters != placement.voters
            || !request.source_membership.learners.is_empty()
            || request.source_membership.log_id.node_id == 0
        {
            return reject("source evidence must match a settled uniform placement");
        }
        // Never accept a membership observation older than the last published
        // verification (or the initial bootstrap certificate).
        let known = self
            .migrations
            .values()
            .filter(|migration| migration.raft_group_id == request.raft_group_id)
            .filter_map(|migration| migration.managed.as_ref())
            .filter(|managed| managed.published_epoch.is_some())
            .filter_map(|managed| {
                managed
                    .final_membership
                    .as_ref()
                    .map(|proof| &proof.membership)
            })
            .max_by_key(|proof| proof.log_id.index)
            .or_else(|| {
                self.cluster_bootstrap
                    .as_ref()?
                    .memberships
                    .get(&request.raft_group_id)
            });
        if let Some(known) = known
            && (!covers(&request.source_membership.log_id, &known.log_id)
                || (request.source_membership.log_id.index == known.log_id.index
                    && &request.source_membership != known))
        {
            return reject("source membership evidence regressed or conflicts");
        }
        let current_policy = self
            .managed_placement
            .as_ref()
            .and_then(|managed| managed.groups.get(&request.raft_group_id));
        if request.target_voters == placement.voters
            && request
                .target_policy
                .as_ref()
                .is_none_or(|policy| Some(policy) == current_policy)
        {
            return reject("migration must change voters or resolved policy");
        }
        let response = self.begin_migration(
            request.raft_group_id,
            request.target_voters.clone(),
            request.target_policy.clone(),
            false,
            now_ms,
        );
        if let ControlResponse::MigrationStarted { migration_id } = response
            && let Some(migration) = self.migrations.get_mut(&migration_id)
        {
            migration.managed = Some(ManagedMigration::new(request));
        }
        response
    }

    pub(crate) fn claim_migration_executor(
        &mut self,
        migration_id: u64,
        expected_generation: u64,
        claim_key: ProcessIncarnation,
        executor: ReceiverProcess,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(migration) = self.migrations.get(&migration_id) else {
            return reject("unknown migration");
        };
        let Some(managed) = &migration.managed else {
            return reject("intent-bound migration required");
        };
        if let Some(assignment) = &managed.executor
            && assignment.claim_key == claim_key
        {
            return if assignment.token.executor == executor
                && assignment.claimed_from_generation == expected_generation
            {
                ControlResponse::ExecutorClaimed {
                    token: assignment.token.clone(),
                }
            } else {
                reject("claim key conflicts with an existing executor request")
            };
        }
        if self.active_migration != Some(migration_id) || !migration.is_running() {
            return reject("migration is not active");
        }
        if !self
            .nodes
            .get(&executor.node_id)
            .is_some_and(|node| matches!(node.state, NodeState::Active | NodeState::Draining))
        {
            return reject("executor node must be a serving registered node");
        }
        if managed
            .executor
            .as_ref()
            .map_or(0, |assignment| assignment.token.generation)
            != expected_generation
        {
            return reject("executor generation CAS failed");
        }
        let generation = self.next_executor_generation;
        let Some(next) = generation.checked_add(1).filter(|_| generation != 0) else {
            return reject("executor generation space exhausted");
        };
        let token = MigrationToken {
            migration_id,
            generation,
            executor,
        };
        let mut updated = migration.clone();
        let Some(managed) = &mut updated.managed else {
            return reject("intent-bound migration required");
        };
        managed.executor = Some(ExecutorAssignment {
            token: token.clone(),
            claim_key,
            claimed_from_generation: expected_generation,
        });
        // A takeover preserves irreversible progress but must certify all live
        // receiving processes and observations again under its new generation.
        managed.receivers.clear();
        managed.prepared.clear();
        managed.learner_applied.clear();
        for status in updated.per_node_learner_status.values_mut() {
            *status = LearnerStatus::Pending;
        }
        managed.released.clear();
        managed.receivers_retired = false;
        managed.verification_generation = None;
        managed.last_update = None;
        // Preserve published proof as historical membership truth; post-publish
        // takeover still must replace it with fresh evidence before cleanup.
        if managed.published_epoch.is_none() {
            managed.final_membership = None;
        }
        updated.updated_at_ms = now_ms;
        self.next_executor_generation = next;
        self.migrations.insert(migration_id, updated);
        ControlResponse::ExecutorClaimed { token }
    }

    pub(crate) fn update_managed_migration(
        &mut self,
        token: MigrationToken,
        expected_revision: u64,
        update: MigrationUpdate,
        now_ms: u64,
    ) -> ControlResponse {
        let Some(current) = self.migrations.get(&token.migration_id) else {
            return reject("unknown migration");
        };
        let Some(managed) = &current.managed else {
            return reject("intent-bound migration required");
        };
        if managed
            .executor
            .as_ref()
            .map(|assignment| &assignment.token)
            != Some(&token)
        {
            return reject("stale executor generation or process");
        }
        if managed.last_update.as_ref() == Some(&(expected_revision, update.clone())) {
            return ControlResponse::Ok;
        }
        if self.active_migration != Some(token.migration_id) || !current.is_running() {
            return reject("migration is not active");
        }
        if managed.revision != expected_revision {
            return reject("migration revision CAS failed");
        }
        let Some(revision) = managed.revision.checked_add(1) else {
            return reject("migration revision space exhausted");
        };
        // Work on a clone so every rejected update is completely atomic.
        let mut migration = current.clone();
        let peers = participants(&migration);
        let Some(managed) = &mut migration.managed else {
            return reject("intent-bound migration required");
        };
        let barrier = managed.receiver_activation_authorized
            && managed.receivers.keys().copied().collect::<BTreeSet<_>>() == peers
            && !managed.receivers_retired;
        if managed.receivers_retired
            && !matches!(
                update,
                MigrationUpdate::Finish | MigrationUpdate::RecordError { .. }
            )
        {
            return reject("retired receiver authority cannot reopen without a new generation");
        }
        match &update {
            MigrationUpdate::AuthorizeReceivers => {
                managed.receiver_activation_authorized = true;
            }
            MigrationUpdate::CertifyReceivers { processes } => {
                if !managed.receiver_activation_authorized
                    || processes.keys().copied().collect::<BTreeSet<_>>() != peers
                    || (!managed.receivers.is_empty() && &managed.receivers != processes)
                {
                    return reject(
                        "receiver barrier must certify exactly the current participant processes",
                    );
                }
                managed.receivers = processes.clone();
                if migration.phase == MigrationPhase::Validating {
                    migration.phase = MigrationPhase::PreparingLocalEngines;
                }
            }
            MigrationUpdate::RecordPrepared { process } => {
                if !barrier
                    || !process_matches(managed, process)
                    || !migration.added_nodes.contains(&process.node_id)
                {
                    return reject("prepared replica lacks a current receiver barrier");
                }
                managed.prepared.insert(process.node_id);
            }
            MigrationUpdate::CapturePrefix { prefix } => {
                if !barrier
                    || managed.prepared != migration.added_nodes
                    || !covers(prefix, &managed.request.source_membership.log_id)
                    || managed
                        .catchup_prefix
                        .as_ref()
                        .is_some_and(|old| old != prefix)
                {
                    return reject(
                        "catch-up prefix requires prepared replicas and one immutable committed prefix",
                    );
                }
                managed.catchup_prefix = Some(prefix.clone());
                if !managed.membership_may_have_changed {
                    migration.phase = MigrationPhase::AddingLearners;
                }
            }
            MigrationUpdate::RecordLearner { evidence } => {
                if !barrier
                    || !managed.prepared.contains(&evidence.process.node_id)
                    || !process_matches(managed, &evidence.process)
                    || !managed
                        .catchup_prefix
                        .as_ref()
                        .is_some_and(|prefix| covers(&evidence.applied_log_id, prefix))
                {
                    return reject(
                        "learner lacks current-process applied evidence through the fixed prefix",
                    );
                }
                managed
                    .learner_applied
                    .insert(evidence.process.node_id, evidence.clone());
                migration
                    .per_node_learner_status
                    .insert(evidence.process.node_id, LearnerStatus::CaughtUp);
            }
            MigrationUpdate::AuthorizeMembership => {
                if !barrier
                    || managed.catchup_prefix.is_none()
                    || managed
                        .learner_applied
                        .keys()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        != migration.added_nodes
                    || managed.published_epoch.is_some()
                {
                    return reject("voter change requires fenced, applied-ready learners");
                }
                managed.membership_may_have_changed = true;
                migration.phase = MigrationPhase::ChangingVoters;
            }
            MigrationUpdate::VerifyMembership { evidence } => {
                if !barrier
                    || !managed.membership_may_have_changed
                    || evidence.membership.voters != migration.target_voters
                    || !evidence.membership.learners.is_empty()
                    || !covers(
                        &evidence.membership.log_id,
                        &managed.request.source_membership.log_id,
                    )
                    || (migration.target_voters != migration.from_voters
                        && evidence.membership.log_id.index
                            <= managed.request.source_membership.log_id.index)
                    || !covers(&evidence.committed_prefix, &evidence.membership.log_id)
                    || !managed
                        .catchup_prefix
                        .as_ref()
                        .is_some_and(|prefix| covers(&evidence.committed_prefix, prefix))
                    || managed.final_membership.as_ref().is_some_and(|old| {
                        !covers(&evidence.membership.log_id, &old.membership.log_id)
                            || (evidence.membership.log_id.index == old.membership.log_id.index
                                && evidence.membership != old.membership)
                    })
                    || evidence.replicas.keys().copied().collect::<BTreeSet<_>>()
                        != migration.target_voters
                    || evidence.replicas.iter().any(|(id, proof)| {
                        *id != proof.process.node_id
                            || !process_matches(managed, &proof.process)
                            || !covers(&proof.applied_log_id, &evidence.committed_prefix)
                    })
                {
                    return reject(
                        "final membership needs uniform target voters and every current target process applied through its post-membership prefix",
                    );
                }
                if managed
                    .final_membership
                    .as_ref()
                    .is_some_and(|old| old.membership != evidence.membership)
                {
                    managed.released.clear();
                }
                managed.final_membership = Some(evidence.clone());
                managed.verification_generation = Some(token.generation);
                if managed.published_epoch.is_none() {
                    migration.phase = MigrationPhase::CommittingPlacement;
                }
            }
            MigrationUpdate::PublishPlacement => {
                if !barrier
                    || managed.final_membership.is_none()
                    || managed.verification_generation != Some(token.generation)
                    || managed.published_epoch.is_some()
                    || migration.phase != MigrationPhase::CommittingPlacement
                {
                    return reject("placement publication requires current verified membership");
                }
                let Some(placement) = self.placements.get(&migration.raft_group_id) else {
                    return reject("group has no placement");
                };
                if placement.epoch != managed.request.expected_epoch
                    || placement.voters != migration.from_voters
                    || self
                        .managed_placement
                        .as_ref()
                        .and_then(|policies| policies.groups.get(&migration.raft_group_id))
                        != migration.from_policy.as_ref()
                {
                    return reject("placement/source policy epoch CAS failed");
                }
                let Some(epoch) = placement.epoch.checked_add(1) else {
                    return reject("placement epoch space exhausted");
                };
                let Some(policy) = &migration.target_policy else {
                    return reject("target policy missing");
                };
                if let Err(reason) = policy.validate_voters(&migration.target_voters, &self.nodes) {
                    return reject(reason);
                }
                managed.published_epoch = Some(epoch);
                migration.phase = MigrationPhase::Finalizing;
            }
            MigrationUpdate::RecordReleased { evidence } => {
                if !barrier
                    || migration.phase != MigrationPhase::Finalizing
                    || managed.verification_generation != Some(token.generation)
                    || !migration.removed_voters.contains(&evidence.process.node_id)
                    || !process_matches(managed, &evidence.process)
                    || Some(evidence.placement_epoch) != managed.published_epoch
                    || !managed
                        .final_membership
                        .as_ref()
                        .is_some_and(|proof| proof.membership.log_id == evidence.membership_log_id)
                    || !evidence.work_drained
                    || !evidence.snapshot_references_retired
                    || !evidence.local_records_reclaimed
                {
                    return reject(
                        "replica retirement needs current intent, membership, process and complete cleanup evidence",
                    );
                }
                managed.released.insert(evidence.process.node_id);
            }
            MigrationUpdate::RetireReceivers { processes } => {
                if !barrier
                    || migration.phase != MigrationPhase::Finalizing
                    || managed.verification_generation != Some(token.generation)
                    || managed.released != migration.removed_voters
                    || &managed.receivers != processes
                {
                    return reject(
                        "executor retirement requires cleanup and every participant's current drained fence",
                    );
                }
                managed.receivers_retired = true;
            }
            MigrationUpdate::Finish => {
                if migration.phase != MigrationPhase::Finalizing
                    || managed.verification_generation != Some(token.generation)
                    || managed.published_epoch.is_none()
                    || managed.released != migration.removed_voters
                    || !managed.receivers_retired
                {
                    return reject(
                        "migration cannot succeed before verified publication, replica cleanup and executor retirement",
                    );
                }
                migration.phase = MigrationPhase::Succeeded;
            }
            MigrationUpdate::Cancel => {
                if managed.receiver_activation_authorized
                    || managed.membership_may_have_changed
                    || managed.published_epoch.is_some()
                {
                    return reject(
                        "possible receiver or membership side effects require reconciliation before unlocking",
                    );
                }
                migration.phase = MigrationPhase::Failed;
            }
            MigrationUpdate::RecordError { reason } => {
                if reason.is_empty() || reason.len() > 4096 {
                    return reject("migration error must contain 1..4096 bytes");
                }
                migration.last_error = Some(reason.clone());
                migration.retry_count = migration.retry_count.saturating_add(1);
            }
        }
        managed.revision = revision;
        managed.last_update = Some((expected_revision, update.clone()));
        migration.updated_at_ms = now_ms;
        if matches!(update, MigrationUpdate::PublishPlacement) {
            let Some(epoch) = managed.published_epoch else {
                return reject("published epoch missing");
            };
            let Some(placement) = self.placements.get_mut(&migration.raft_group_id) else {
                return reject("group has no placement");
            };
            placement.voters = migration.target_voters.clone();
            placement.learners.clear();
            placement.draining = migration.removed_voters.clone();
            placement.epoch = epoch;
            placement.updated_at_ms = now_ms;
            if let (Some(policies), Some(policy)) =
                (&mut self.managed_placement, &migration.target_policy)
            {
                policies
                    .groups
                    .insert(migration.raft_group_id, policy.clone());
            }
        }
        if matches!(update, MigrationUpdate::RecordReleased { .. })
            && let Some(placement) = self.placements.get_mut(&migration.raft_group_id)
        {
            placement.draining = migration
                .removed_voters
                .difference(&managed.released)
                .copied()
                .collect();
            placement.updated_at_ms = now_ms;
        }
        if !migration.is_running() {
            self.active_migration = None;
        }
        self.migrations.insert(token.migration_id, migration);
        ControlResponse::Ok
    }
}
