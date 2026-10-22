//! The one conversion from OpenRaft's errors to the group engine's error.
//!
//! OpenRaft's errors are not serializable, and their text names internal
//! node addresses (a quorum error lists every member). Each conversion here
//! logs the native error once and answers with the variant a caller acts
//! on: a leadership refusal for a forward and for a write that never
//! reached a core that stopped, `OutcomeUnknown` for a write whose answer
//! was lost (a core that stopped after it took the write included), and the
//! setup variants for a group that does not start. A
//! core that stopped before anything was asked of it (`Fatal`) is
//! `OwnerStopped`, through [`owner_stopped`], the conversion every owner
//! call uses.

use std::time::Duration;

use openraft::Config;
use openraft::error::ClientWriteError;
#[cfg(all(test, not(madsim)))]
use openraft::error::InitializeError;
use openraft::error::LinearizableReadError;
use openraft::error::RaftError;
use openraft::errors::Fatal;
use openraft::errors::ForwardReason;
use openraft::metrics::WaitError;
use openraft::raft::WriteResult;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::LeadershipReason;
#[cfg(all(test, not(madsim)))]
use ursula_runtime::MembershipInitRefusal;
use ursula_runtime::Proposal;
use ursula_runtime::ResponseKind;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use crate::GroupLeadership;
use crate::forward::leadership_refusal;
use crate::forward::unknown_leader;
use crate::owner::owner_stopped;
use crate::types::UrsulaRaftTypeConfig;

type C = UrsulaRaftTypeConfig;

/// `config` once OpenRaft validates it. A config it refuses is the group's
/// [`GroupInfraError::InvalidRaftConfig`], which names the timings.
pub(crate) fn validated_config(config: Config) -> Result<Config, GroupEngineError> {
    let heartbeat_ms = config.heartbeat_interval;
    let election_timeout_min_ms = config.election_timeout_min;
    let election_timeout_max_ms = config.election_timeout_max;
    config.validate().map_err(|source| {
        tracing::error!(error = %source, "OpenRaft refused the group's Raft config");
        GroupEngineError::Infra(GroupInfraError::InvalidRaftConfig {
            heartbeat_ms,
            election_timeout_min_ms,
            election_timeout_max_ms,
        })
    })
}

/// OpenRaft's refusal to initialize the membership of `raft_group_id`, for
/// the unit tests that initialize a group by hand
/// ([`crate::RaftGroupEngine::initialize_membership`]).
#[cfg(all(test, not(madsim)))]
pub(crate) fn initialize_error(
    raft_group_id: RaftGroupId,
    error: RaftError<C, InitializeError<C>>,
) -> GroupEngineError {
    let refusal = match &error {
        RaftError::Fatal(fatal) => return owner_stopped(fatal),
        RaftError::APIError(InitializeError::NotAllowed(_)) => MembershipInitRefusal::NotAllowed,
        RaftError::APIError(InitializeError::NotInMembers(_)) => {
            MembershipInitRefusal::NotInMembers
        }
    };
    tracing::error!(raft_group_id = raft_group_id.0, %error, "OpenRaft refused to initialize the group's membership");
    GroupEngineError::Infra(GroupInfraError::MembershipInit {
        group: RaftTarget::Data(raft_group_id),
        refusal,
    })
}

/// A wait of `raft_group_id`, at most `timeout`, for an expected leader that
/// did not see it.
pub(crate) fn leader_wait_error(
    raft_group_id: RaftGroupId,
    timeout: Duration,
    error: WaitError,
) -> GroupEngineError {
    match error {
        WaitError::Timeout(..) => {
            tracing::warn!(raft_group_id = raft_group_id.0, %error, "the group observed no expected leader in time");
            GroupEngineError::Infra(GroupInfraError::LeaderWaitTimedOut {
                group: RaftTarget::Data(raft_group_id),
                timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            })
        }
        WaitError::ShuttingDown => owner_stopped(error),
    }
}

/// A write whose submission did not reach RaftCore. `Raft::write` fails
/// only when RaftCore stopped and its queue handed the message back unsent,
/// so nothing of the write was proposed: the replica no longer leads, and no
/// leader is known to it.
pub(crate) fn write_not_submitted(fatal: Fatal<C>) -> GroupEngineError {
    tracing::warn!(error = %fatal, "Raft write not submitted: the Raft core stopped");
    unknown_leader(LeadershipReason::SteppedDown, Proposal::NotProposed)
}

/// A write RaftCore took whose answer never came: the core stopped after it
/// took the write, which may have committed and applies after repair.
pub(crate) fn write_answer_lost() -> GroupEngineError {
    tracing::warn!("Raft write response lost after submission");
    GroupEngineError::Infra(GroupInfraError::OutcomeUnknown)
}

/// A write RaftCore took while this replica led, whose leadership lapsed
/// before the write was answered ([`crate::leadership::until_lapsed`]): the
/// entry may be in the log of a quorum and commit through a newer leader, or
/// commit here once the leader hears from its peers again, so its outcome is
/// unknown and its key resolves it.
pub(crate) fn write_outlived_leadership<T>() -> Result<T, GroupEngineError> {
    tracing::debug!("a lapsed leader stopped waiting for a write it proposed");
    Err(unknown_leader(
        LeadershipReason::QuorumUnconfirmed,
        Proposal::MayHaveCommitted,
    ))
}

/// Map OpenRaft's refusal of a proposed write (RT1). OpenRaft forwards a
/// write only before it appends the entry: on a replica that does not lead,
/// a leader whose quorum lease expired, or one that is handing its
/// leadership over. Nothing of the write applied, so the answer is a
/// redirect or `NO_LEADER`. Only an entry OpenRaft appended and then
/// discarded for another leader's log may still commit through that leader,
/// so that refusal stays ambiguous. A normal write cannot fail a membership
/// change or a precondition, so either is logged as a bug and answered as an
/// unknown outcome.
pub(crate) fn client_write_refusal(
    err: &ClientWriteError<C>,
    leadership: &GroupLeadership,
) -> GroupEngineError {
    match err {
        ClientWriteError::ForwardToLeader(forward) => {
            tracing::debug!(error = %err, "OpenRaft refused a write before appending it");
            leadership_refusal(
                forward_reason(forward.reason, LeadershipReason::SteppedDown),
                Proposal::NotProposed,
                forward.leader_id,
                leadership,
            )
        }
        ClientWriteError::LogEntryDiscarded(forward) => {
            tracing::debug!(error = %err, "OpenRaft discarded a proposed write");
            leadership_refusal(
                LeadershipReason::SteppedDown,
                Proposal::MayHaveCommitted,
                forward.leader_id,
                leadership,
            )
        }
        ClientWriteError::ChangeMembershipError(_) | ClientWriteError::PreconditionFailed(_) => {
            tracing::error!(error = %err, "OpenRaft refused a normal write with a membership or precondition error");
            GroupEngineError::Infra(GroupInfraError::OutcomeUnknown)
        }
    }
}

/// Why OpenRaft forwarded a request, as a leadership refusal says it.
/// `not_leader` is the reason for a replica that does not lead.
fn forward_reason(reason: ForwardReason, not_leader: LeadershipReason) -> LeadershipReason {
    match reason {
        ForwardReason::NotLeader => not_leader,
        ForwardReason::LeaseExpired => LeadershipReason::QuorumUnconfirmed,
        ForwardReason::LeadershipTransfer => LeadershipReason::LeadershipTransfer,
    }
}

/// Map a failed ReadIndex barrier (`Raft::get_read_linearizer`). Nothing was
/// proposed. A lost leadership forwards to the known leader (503 while it is
/// unknown), and a leader that cannot reach a quorum answers leader-unknown
/// (503) rather than serving a view that may miss a newer leader's
/// acknowledged writes. A stopped core is the stopped owner.
///
/// The answer names only the reason; a refusal is logged at debug level
/// only.
pub(crate) fn group_engine_linearizable_read_error(
    err: RaftError<C, LinearizableReadError<C>>,
    operation: &'static str,
    leadership: &GroupLeadership,
) -> GroupEngineError {
    let refusal = match err {
        RaftError::Fatal(fatal) => return owner_stopped(fatal),
        RaftError::APIError(refusal) => refusal,
    };
    tracing::debug!("OpenRaft {operation} could not confirm leadership: {refusal}");
    match &refusal {
        LinearizableReadError::ForwardToLeader(forward) => leadership_refusal(
            forward_reason(forward.reason, LeadershipReason::Follower),
            Proposal::NotProposed,
            forward.leader_id,
            leadership,
        ),
        LinearizableReadError::QuorumNotEnough(_) => {
            unknown_leader(LeadershipReason::QuorumUnconfirmed, Proposal::NotProposed)
        }
    }
}

/// One answer of a batch write ([`crate::forward::write_commands_on_raft`]):
/// the state machine's outcome of a command whose response is of the kind
/// `expected`, or OpenRaft's refusal of it as `leadership` sees it.
pub(crate) fn batch_write_result<SM: crate::replica::ReplicaStateMachine>(
    result: WriteResult<C>,
    expected: ResponseKind,
    leadership: impl FnOnce() -> GroupLeadership,
) -> Result<SM::Answer, GroupEngineError> {
    match result {
        Ok(response) => SM::answer(response.response, expected)?,
        Err(refusal) => Err(client_write_refusal(&refusal, &leadership())),
    }
}
