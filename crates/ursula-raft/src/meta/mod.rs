//! The meta group's replicated types: the commands its log carries inside
//! [`crate::RaftCommand::Meta`], the state its state machine holds and
//! snapshots, and the answers it gives.
//!
//! Every type of this module itself but [`MetaForwardError`], which stays
//! in the forwarding process, and of [`genesis`] and [`snapshot`], is
//! format (format epoch 6): persisted in the meta log and its snapshot
//! record, replicated, or sent between nodes, so its field order and
//! variant names are the encoding (`ursula_stream::msgpack`). No type here
//! uses `default`, `skip_serializing_if`, `flatten` or an untagged enum. The
//! persisted-envelopes and commands-and-responses fixtures pin each root.
//! The replica, its state machine and its views ([`replica`],
//! [`state_machine`], [`watch`]) encode nothing of their own, and
//! [`state_machine`]'s dispatch of the meta log is frozen with the epoch.
//! The runtime and its tasks ([`runtime`], [`handle`], [`genesis_driver`],
//! [`registration`], [`formation`], [`damage`] and the meta membership
//! target) encode nothing either: they write only the commands above.
//!
//! `#[serde(other)]` is allowed only on admin JSON enums (the admin
//! vocabulary of `ursula_proto::placement`), and forbidden on every type
//! here and on every other type of the replicated, persisted or RPC
//! MessagePack closure: an unknown variant decoding as "other" would hide a
//! format mismatch.
//!
//! The Raft gRPC service routes the meta plane's RPCs to the replica the
//! registry publishes ([`replica::ActiveMeta`]), which the meta runtime
//! ([`runtime::MetaRuntime`]) opens. No server starts the runtime yet.
//!
//! Module map:
//!
//! - [`confirm`]: why a meta read barrier confirmed no placement view
//!   ([`confirm::MetaBarrierError`]), never encoded.
//! - [`damage`]: which refusals of the commit screen damage the meta
//!   replica for good.
//! - [`formation`]: the meta leader's `Bootstrap` of the cluster.
//! - [`genesis`]: the genesis probe a node sends before the meta group
//!   forms, and its answer.
//! - [`genesis_driver`]: the meta replica's gate drivers with the genesis
//!   probe, and what the probes last heard ([`genesis_driver::GenesisStatus`]).
//! - [`handle`]: how this node writes to the meta group and confirms its
//!   view ([`handle::MetaHandle`], [`handle::MetaWriteError`]).
//! - [`registration`]: this node's registration with the meta group.
//! - [`replica`]: this node's meta replica ([`replica::MetaReplica`]): its
//!   Raft over the meta journal, the tasks it owns, its registry slot
//!   ([`replica::ActiveMeta`]) and its restart for a recovery plan.
//! - [`runtime`]: the meta runtime ([`runtime::MetaRuntime`]): the replica
//!   with every task it owns, the node tasks and the damage supervisor.
//! - [`snapshot`]: the meta group's snapshot pointer, always `Inline`, the
//!   bound on its size, and the trigger that snapshots the meta group on
//!   its journal's age rule.
//! - [`state_machine`]: the meta state machine ([`MetaStateMachine`]): its
//!   dispatch of the meta log, its inline snapshot record and the views it
//!   publishes.
//! - `target`: the meta group's membership target, which the reconcile
//!   shell pursues on the meta leader.
//! - [`watch`]: the node's placement views, which outlive every meta
//!   replica, and one group's placement in them.

pub mod confirm;
pub mod damage;
pub mod formation;
pub mod genesis;
pub mod genesis_driver;
pub mod handle;
pub mod registration;
pub mod replica;
pub mod runtime;
pub mod snapshot;
pub mod state_machine;
mod target;
pub mod watch;

use std::num::NonZeroU64;

use serde::Deserialize;
use serde::Serialize;
pub use state_machine::MetaStateMachine;

#[cfg(test)]
#[cfg(not(madsim))]
mod grpc_tests;
#[cfg(test)]
#[cfg(not(madsim))]
mod runtime_tests;
#[cfg(test)]
#[cfg(not(madsim))]
mod tests;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;
use ursula_control::HistoryCause;
use ursula_runtime::ErrorStatus;
use ursula_runtime::GroupEngineError;
use ursula_runtime::WriteEffect;
use ursula_stream::HistoryEvent;
use ursula_stream::HistoryLeader;

use crate::ForwardError;

/// A command of the meta group's log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaCommand {
    /// The meta group's history event, proposed only by the meta leader's
    /// history driver: at the genesis and for each accepted recovery plan.
    /// Its fields are those of the data groups' `StreamCommand::NewHistory`,
    /// in its order, without its stream clock, and with what began the
    /// history. An event whose `previous` is not the meta group's current
    /// history applies nothing.
    NewHistory {
        /// The new history's id, drawn by the proposer.
        history: NonZeroU64,
        /// The id of the history this one replaces, as the proposer applied
        /// it; 0 at the genesis, which replaces none.
        previous: u64,
        /// The last log index the new history keeps from the one it
        /// replaces.
        kept: u64,
        /// The proposer's term and node id: the leader id of the entry that
        /// carries the event.
        leader: HistoryLeader,
        cause: HistoryCause,
    },
    /// A command of the placement kernel.
    Control(ControlCommand),
}

/// The command's name in logs.
impl std::fmt::Display for MetaCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewHistory { history, cause, .. } => {
                write!(f, "meta_new_history:{history:#x}:{cause:?}")
            }
            Self::Control(command) => write!(f, "meta_control:{command:?}"),
        }
    }
}

/// Why a control command forwarded to the meta leader got no answer of the
/// meta group's. Never encoded: the forwarding node builds it from its
/// transport's answer, over gRPC or in process alike.
#[derive(Debug, thiserror::Error)]
pub enum MetaForwardError {
    /// The meta leader's refusal of the command, or of the whole write.
    #[error("the meta leader refused the forwarded control command")]
    Leader(#[source] GroupEngineError),
    /// The forward got no answer of the meta group: the leader was not
    /// reached, or refused the request itself.
    #[error("the forwarded control command got no answer from the meta leader")]
    Transport(#[source] ForwardError),
}

impl MetaForwardError {
    /// What the forwarded command did.
    pub fn write_effect(&self) -> WriteEffect {
        match self {
            Self::Leader(error) => error.write_effect(),
            Self::Transport(error) => error.write_effect(),
        }
    }

    /// Whether forwarding the command again may succeed.
    pub fn status(&self) -> ErrorStatus {
        match self {
            Self::Leader(error) => error.status(),
            Self::Transport(error) => error.status(),
        }
    }

    /// The answer when the meta leader cannot be reached, so nothing was
    /// sent: a leader this node does not know, nothing proposed.
    pub fn leader_unreachable() -> Self {
        Self::Leader(crate::forward::unknown_leader(
            ursula_runtime::LeadershipReason::LeaderUnreachable,
            ursula_runtime::Proposal::NotProposed,
        ))
    }

    /// The refusal `status` of the meta leader `leader` answered in this
    /// process, classified as the same refusal over gRPC is: the in-process
    /// seam's answer to a write the leader refused itself, such as a
    /// `RegisterNode` naming another node than the sender.
    #[cfg(any(test, madsim))]
    pub(crate) fn refused_in_process(leader: u64, status: tonic::Status) -> Self {
        Self::Transport(ForwardError::Refused {
            rpc: ursula_runtime::ForwardedRpc::GroupWrite,
            address: format!("in-process node {leader}"),
            source: status,
        })
    }
}

/// The meta group's state machine value, which its snapshot holds whole.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetaState {
    /// The history event the meta group applied last; `None` before its
    /// genesis. The data groups' type, unchanged.
    pub history: Option<HistoryEvent>,
    pub control: ControlPlaneState,
}

/// What the meta state machine answers an applied [`MetaCommand`]. Never
/// persisted or sent: a forwarded control command is answered with its
/// [`ControlResponse`], and an arm reaches the wire only as an
/// `ursula_runtime::ResponseKind`. It derives serde as OpenRaft requires of
/// a response type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaResponse {
    /// A history event applied.
    History,
    /// A history event named another `previous` than the `current` history
    /// and applied nothing.
    HistoryMismatch { current: u64 },
    /// The placement kernel's answer to a control command.
    Control(ControlResponse),
}
