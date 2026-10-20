//! The meta group's replicated types: the commands its log carries inside
//! [`crate::RaftCommand::Meta`], the state its state machine holds and
//! snapshots, and the answers it gives.
//!
//! Every type here is format (format epoch 6): persisted in the meta log and
//! its snapshot record, replicated, or sent between nodes, so its field
//! order and variant names are the encoding (`ursula_stream::msgpack`). No
//! type here uses `default`, `skip_serializing_if`, `flatten` or an untagged
//! enum. The persisted-envelopes and commands-and-responses fixtures pin
//! each root.
//!
//! Nothing runs the meta group yet: this module is its format, and the
//! state machine, the journal and the transport that use it land with the
//! meta runtime.
//!
//! Module map:
//!
//! - [`genesis`]: the genesis probe a node sends before the meta group
//!   forms, and its answer.
//! - [`snapshot`]: the meta group's snapshot pointer, always `Inline`, and
//!   the bound on its size.

pub mod genesis;
pub mod snapshot;

use std::num::NonZeroU64;

use serde::Deserialize;
use serde::Serialize;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;
use ursula_control::HistoryCause;
use ursula_stream::HistoryEvent;
use ursula_stream::HistoryLeader;

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
