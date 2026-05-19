use openraft::rt::WatchReceiver;
use openraft::RaftNetworkV2;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;
use ursula_runtime::GroupEngineError;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::state_machine::*;
use crate::types::*;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetworkFactory;

#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeRaftNetwork;

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for SingleNodeRaftNetworkFactory {
    type Network = SingleNodeRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        SingleNodeRaftNetwork
    }
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for SingleNodeRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send AppendEntries")
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send Vote")
    }

    async fn full_snapshot(
        &mut self,
        _vote: VoteOf<UrsulaRaftTypeConfig>,
        _snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        unreachable!("single-node raft group must not send snapshots")
    }
}

#[derive(Debug, Clone, Default)]
pub struct RaftGroupHandleRegistry {
    groups: Arc<Mutex<BTreeMap<u32, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
}

impl RaftGroupHandleRegistry {
    pub fn register(
        &self,
        placement: ShardPlacement,
        raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    ) {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .insert(placement.raft_group_id.0, raft);
    }

    pub fn get(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .get(&raft_group_id.0)
            .cloned()
    }

    pub fn contains_group(&self, raft_group_id: RaftGroupId) -> bool {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .contains_key(&raft_group_id.0)
    }

    pub fn len(&self) -> usize {
        self.groups
            .lock()
            .expect("raft group handle registry mutex")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn metrics_snapshot(&self) -> Vec<RaftGroupMetricsSnapshot> {
        let groups = self
            .groups
            .lock()
            .expect("raft group handle registry mutex")
            .iter()
            .map(|(raft_group_id, raft)| (*raft_group_id, raft.clone()))
            .collect::<Vec<_>>();

        let mut snapshots = Vec::with_capacity(groups.len());
        for (raft_group_id, raft) in groups {
            let metrics = raft.metrics().borrow_watched().clone();
            let membership = metrics.membership_config.membership();
            snapshots.push(RaftGroupMetricsSnapshot {
                raft_group_id,
                node_id: metrics.id,
                current_term: metrics.current_term,
                current_leader: metrics.current_leader,
                last_log_index: metrics.last_log_index,
                committed: metrics.committed.map(log_progress_snapshot),
                last_applied: metrics.last_applied.map(log_progress_snapshot),
                snapshot: metrics.snapshot.map(log_progress_snapshot),
                purged: metrics.purged.map(log_progress_snapshot),
                voter_ids: membership.voter_ids().collect(),
                learner_ids: membership.learner_ids().collect(),
            });
        }
        snapshots
    }

    pub async fn append_entries(
        &self,
        raft_group_id: RaftGroupId,
        request: AppendEntriesRequest<UrsulaRaftTypeConfig>,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        raft.append_entries(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft AppendEntries: {err}")))
    }

    pub async fn vote(
        &self,
        raft_group_id: RaftGroupId,
        request: VoteRequest<UrsulaRaftTypeConfig>,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        raft.vote(request)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft Vote: {err}")))
    }

    pub async fn install_full_snapshot(
        &self,
        raft_group_id: RaftGroupId,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft install snapshot: {err}")))
    }

    pub async fn build_snapshot_for_transfer(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<TypeConfigSnapshotOf<UrsulaRaftTypeConfig>, GroupEngineError> {
        let raft = self.require_group(raft_group_id)?;
        let snapshot = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    let mut builder = state_machine.get_snapshot_builder().await;
                    builder.build_snapshot().await
                })
            })
            .await
            .map_err(|err| GroupEngineError::new(format!("OpenRaft build snapshot: {err}")))?
            .map_err(|err| GroupEngineError::new(format!("build OpenRaft snapshot: {err}")))?;
        Ok(snapshot)
    }

    fn require_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>, GroupEngineError> {
        self.get(raft_group_id).ok_or_else(|| {
            GroupEngineError::new(format!(
                "raft group {} is not registered on this node",
                raft_group_id.0
            ))
        })
    }
}

pub(crate) fn log_progress_snapshot(
    log_id: LogIdOf<UrsulaRaftTypeConfig>,
) -> RaftLogProgressSnapshot {
    RaftLogProgressSnapshot {
        term: log_id.leader_id.term,
        index: log_id.index,
    }
}
