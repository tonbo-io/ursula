//! Native recovery probes, outside the engine construction boundary.
use std::time::Duration;

use openraft::RaftNetworkV2;
use openraft::network::RPCOption;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;

use crate::BarrierAnswer;
use crate::forward::LeaderRoute;
use crate::grpc::GrpcRaftNetwork;
use crate::grpc::probe_rejoin_vote_barrier;
use crate::meta::MetaForwardError;
use crate::meta::genesis::GenesisProbe;
use crate::rejoin::PeerGroupLog;
use crate::rejoin::RecoveryTransport;
use crate::rejoin::bootstrap_probe_vote;
use crate::target::RaftTarget;

/// Recovery probes over gRPC: the bootstrap probe vote about group
/// `target`, and the genesis probe, barriers and forwarded control commands
/// of any plane. A peer is dialed at the cluster URL `raft.peers` lists for
/// it in `transport`'s peer cluster. A peer it does not list is not probed.
#[derive(Clone)]
pub(crate) struct GrpcRecoveryTransport {
    pub transport: std::sync::Arc<crate::grpc::CoreRaftTransport>,
    pub target: RaftTarget,
    pub timeout: Duration,
}
impl RecoveryTransport for GrpcRecoveryTransport {
    type Error = crate::grpc::RecoveryProbeError;
    async fn probe(&self, peer: u64) -> Option<PeerGroupLog> {
        GrpcRaftNetwork::new(self.transport.clone(), self.target, peer)
            .vote(bootstrap_probe_vote(), RPCOption::new(self.timeout))
            .await
            .ok()
            .map(|response| PeerGroupLog::from_vote_response(&response))
    }
    async fn genesis_probe(&self, peer: u64, probe: &GenesisProbe) -> Option<PeerGroupLog> {
        crate::grpc::genesis_probe(self.transport.clone(), peer, probe, self.timeout).await
    }
    async fn barrier(&self, leader: u64, target: RaftTarget) -> Result<BarrierAnswer, Self::Error> {
        probe_rejoin_vote_barrier(
            self.transport.clone(),
            target,
            leader,
            crate::rejoin::RECOVERY_BARRIER_TIMEOUT,
        )
        .await
    }
    async fn forward_control(
        &self,
        leader: u64,
        command: ControlCommand,
    ) -> Result<ControlResponse, MetaForwardError> {
        let peer_cluster = self.transport.peer_cluster.clone();
        let Some(address) = peer_cluster.cluster_url(leader) else {
            return Err(MetaForwardError::leader_unreachable());
        };
        let route = LeaderRoute {
            peer_cluster,
            leader_id: leader,
            address,
        };
        crate::forward::forward_control_to_leader(&route, command).await
    }
}
