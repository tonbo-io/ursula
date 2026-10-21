//! The meta replica's gate drivers, with the genesis probe (design 3.5).
//!
//! A meta replica opens its recovery gate through the drivers every replica
//! runs (`RecoveryGate::spawn_gate_drivers`): the barrier driver, the
//! genesis vote floor task and, on the smallest claimed meta voter while
//! the replica holds no meta history, the bootstrap driver. Meta's drivers
//! ask every `raft.peers` node with the genesis probe instead of the data
//! plane's probe vote, and the bootstrap driver initializes the claimed
//! meta voters once every other peer answered empty with a floor and no
//! leader. A single node, whose peers are itself, initializes at once. The
//! replica's history driver then proposes the genesis history event.
//!
//! Every probe answer updates the node's [`GenesisStatus`], which status
//! reports read.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::NodeId;

use super::genesis::GenesisProbe;
use crate::PeerGroupLog;
use crate::RecoveryTransport;
use crate::rejoin::GateProbe;
use crate::rejoin::bootstrap_probe_vote;
use crate::rt::sync::watch;

/// What the genesis probes last heard from the other `raft.peers` nodes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GenesisStatus {
    /// The peers whose last answer did not arrive, or was not empty.
    pub unanswered: BTreeSet<NodeId>,
    /// The peers whose last answer claims other genesis values, with them.
    pub mismatch: BTreeMap<NodeId, GenesisClaim>,
}

impl GenesisStatus {
    /// The status before any answer: every other peer of `claim` is
    /// unanswered.
    pub(crate) fn before_answers(me: NodeId, claim: &GenesisClaim) -> Self {
        Self {
            unanswered: claim.peers.iter().filter(|peer| *peer != me).collect(),
            mismatch: BTreeMap::new(),
        }
    }

    /// Records `peer`'s answer, `true` when the status changed.
    fn record(&mut self, peer: NodeId, answer: Option<&PeerGroupLog>) -> bool {
        let before = self.clone();
        match answer {
            Some(PeerGroupLog::Empty) => {
                self.unanswered.remove(&peer);
                self.mismatch.remove(&peer);
            }
            Some(PeerGroupLog::GenesisMismatch { claim }) => {
                self.unanswered.insert(peer);
                self.mismatch.insert(peer, claim.clone());
            }
            None | Some(PeerGroupLog::Unprepared | PeerGroupLog::Initialized) => {
                self.unanswered.insert(peer);
                self.mismatch.remove(&peer);
            }
        }
        *self != before
    }
}

/// The peers meta's gate drivers ask: every `raft.peers` node, the meta
/// learners included, so a node that holds meta history but is no meta
/// voter holds a genesis back. A negative control turns it off
/// (`Defence::GenesisEveryPeer`), and the drivers then ask the claimed meta
/// voters only.
pub(crate) fn probed_peers(claim: &GenesisClaim) -> BTreeSet<u64> {
    use ursula_stream::sim_controls::Defence;
    let peers = if ursula_stream::sim_controls::off(Defence::GenesisEveryPeer) {
        ursula_stream::sim_controls::note_fired(Defence::GenesisEveryPeer);
        claim.meta_voters.to_set()
    } else {
        claim.peers.to_set()
    };
    peers.into_iter().map(NodeId::get).collect()
}

/// The gate drivers' probe of the meta plane: the genesis probe with
/// `claim`, each answer recorded in `status`.
pub(crate) fn genesis_gate_probe<T: RecoveryTransport>(
    transport: T,
    claim: GenesisClaim,
    status: watch::Sender<GenesisStatus>,
) -> GateProbe {
    let probe = Arc::new(GenesisProbe {
        claim,
        vote: bootstrap_probe_vote(),
    });
    Arc::new(move |peer| {
        let transport = transport.clone();
        let probe = Arc::clone(&probe);
        let status = status.clone();
        Box::pin(async move {
            let answer = transport.genesis_probe(peer, &probe).await;
            status.send_if_modified(|status| status.record(NodeId::new(peer), answer.as_ref()));
            answer
        })
    })
}
