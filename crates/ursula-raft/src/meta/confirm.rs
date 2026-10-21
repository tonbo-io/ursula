//! Why a meta read barrier did not confirm a placement view.
//!
//! A reader takes a placement view as current only with a confirmation
//! from a meta read barrier (`ursula_control::ConfirmedStamp`). These are
//! the ways that barrier fails. They stay inside one process: nothing here
//! is encoded, and a remote leader's refusal reaches it already classified
//! by the transport ([`BarrierFailure`]).

use std::time::Duration;

use ursula_control::UnconfirmedView;
use ursula_proto::placement::NodeId;

/// Why a meta read barrier confirmed no view.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetaBarrierError {
    /// No meta leader is known to ask.
    #[error("the meta group has no known leader")]
    NoLeader,
    /// The local meta replica stopped.
    #[error("the local meta replica stopped")]
    Stopped,
    /// The barrier completed, and the local view does not carry it.
    #[error("the local placement view does not carry the meta barrier")]
    Unconfirmed {
        #[source]
        source: UnconfirmedView,
    },
    /// The local replica did not apply up to the barrier in time.
    #[error("the local meta replica did not reach the meta barrier within {waited:?}")]
    TimedOut { waited: Duration },
    /// The meta leader asked did not answer with a barrier.
    #[error("the meta leader {leader} gave no barrier: {failure}")]
    Barrier {
        leader: NodeId,
        failure: BarrierFailure,
    },
}

/// How a meta leader's barrier failed, as the transport classifies it.
/// Every recovery transport's error converts into it
/// ([`crate::RecoveryTransport::Error`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BarrierFailure {
    /// The leader could not be reached.
    #[error("unreachable")]
    Unreachable,
    /// The leader's address serves another node now.
    #[error("its address serves node {node}")]
    OtherNode { node: NodeId },
    /// The node asked no longer leads the meta group.
    #[error("it does not lead the meta group")]
    NotLeader,
    /// The answer did not decode.
    #[error("its answer is malformed")]
    Malformed,
    /// The leader refused the barrier.
    #[error("it refused the barrier ({code:?})")]
    Refused { code: tonic::Code },
}

impl BarrierFailure {
    /// The failure a barrier call answered with `status` reads as: the
    /// leader unreachable when the call failed at the transport or the
    /// leader could not serve it, else the leader's refusal.
    pub(crate) fn of_status(status: &tonic::Status) -> Self {
        if crate::forward::is_transport_failure(status) {
            return Self::Unreachable;
        }
        Self::Refused {
            code: status.code(),
        }
    }
}

/// A barrier the leader refused in this process, classified as the same
/// refusal over gRPC is (its one status conversion).
impl From<crate::QuorumProofError> for BarrierFailure {
    fn from(error: crate::QuorumProofError) -> Self {
        Self::of_status(&tonic::Status::from(error))
    }
}
