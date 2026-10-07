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

#[derive(Clone)]
pub(crate) struct GrpcRecoveryTransport {
    pub placement: ShardPlacement,
    pub node_id: u64,
    pub timeout: Duration,
}
impl RecoveryTransport for GrpcRecoveryTransport {
    type Error = String;
    async fn probe(&self, peer: u64, address: String) -> Option<PeerGroupLog> {
        GrpcRaftNetwork::new(self.placement.raft_group_id, peer, &address)
            .vote(
                bootstrap_probe_vote(self.node_id),
                RPCOption::new(self.timeout),
            )
            .await
            .ok()
            .map(|response| PeerGroupLog::from_vote_response(&response))
    }
    async fn barrier(
        &self,
        leader: u64,
        address: String,
    ) -> Result<(UrsulaVote, u64), Self::Error> {
        probe_rejoin_vote_barrier(
            self.placement,
            self.node_id,
            leader,
            &address,
            Duration::from_secs(3),
        )
        .await
    }
}
