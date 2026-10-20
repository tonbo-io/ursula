//! Native recovery probes, outside the engine construction boundary.
use std::time::Duration;

use openraft::RaftNetworkV2;
use openraft::network::RPCOption;
use ursula_shard::ShardPlacement;

use crate::UrsulaVote;
use crate::grpc::GrpcRaftNetwork;
use crate::grpc::probe_rejoin_vote_barrier;
use crate::rejoin::PeerGroupLog;
use crate::rejoin::RecoveryTransport;
use crate::rejoin::bootstrap_probe_vote;

/// Recovery probes over gRPC. A peer is dialed at the cluster URL
/// `raft.peers` lists for it in `transport`'s peer cluster. A peer it does
/// not list is not probed.
#[derive(Clone)]
pub(crate) struct GrpcRecoveryTransport {
    pub transport: std::sync::Arc<crate::grpc::CoreRaftTransport>,
    pub placement: ShardPlacement,
    pub timeout: Duration,
}
impl RecoveryTransport for GrpcRecoveryTransport {
    type Error = crate::grpc::RecoveryProbeError;
    async fn probe(&self, peer: u64) -> Option<PeerGroupLog> {
        GrpcRaftNetwork::new(self.transport.clone(), self.placement.raft_group_id, peer)
            .vote(bootstrap_probe_vote(), RPCOption::new(self.timeout))
            .await
            .ok()
            .map(|response| PeerGroupLog::from_vote_response(&response))
    }
    async fn barrier(&self, leader: u64) -> Result<(UrsulaVote, u64), Self::Error> {
        let answer = probe_rejoin_vote_barrier(
            self.transport.clone(),
            self.placement,
            leader,
            crate::rejoin::RECOVERY_BARRIER_TIMEOUT,
        )
        .await?;
        Ok((answer.vote, answer.index))
    }
}
