//! How this node writes to the meta group and confirms its placement view
//! (design 3.7, 3.8), whether or not it is a meta member.
//!
//! [`MetaHandle`] resolves the registry's meta replica on every call, so it
//! outlives a replica's restart. A write goes to the meta leader: in place
//! when this node leads, else forwarded to the leader its replica names.
//! A node whose replica names no leader, as every node that is not a meta
//! member yet, walks the meta candidates
//! (`ursula_control::leader_candidates`, meta's voters first), following
//! one `ForwardToLeader` hop, and the answer returns on the request's
//! connection.
//!
//! A view is confirmed by a meta read barrier: on the leader its own, after
//! its history fence, elsewhere the leader's over the recovery transport.
//! The barrier bounds the meta log by the leader's committed vote and an
//! index, and the call waits until the local view applied an entry at
//! least the bound, compared term first, since by index alone a learner
//! that missed a meta plan could confirm a pre-plan view.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use ursula_control::CandidatesOf;
use ursula_control::ConfirmedStamp;
use ursula_control::ConfirmedView;
use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::PlacementView;
use ursula_control::UnconfirmedView;
use ursula_control::leader_candidates;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::ViewStage;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WriteEffect;
use ursula_runtime::GroupEngineError;

use super::MetaCommand;
use super::MetaForwardError;
use super::confirm::BarrierFailure;
use super::confirm::MetaBarrierError;
use super::replica::ActiveMeta;
use super::replica::WriteOrigin;
use super::root::RootGuard;
use super::watch::PlacementWatch;
use crate::BarrierAnswer;
use crate::ForwardError;
use crate::RaftGroupHandleRegistry;
use crate::RaftTarget;
use crate::RecoveryTransport;
use crate::memberships::MembershipKind;
use crate::memberships::membership_config;
use crate::types::RaftCommand;

/// How long a confirmation waits for the local view to reach the barrier.
const CONFIRM_WAIT: Duration = Duration::from_secs(10);

/// Why a meta write did not answer with its result. It stays in this
/// process: a node builds it from its own replica's answer or from its
/// transport's.
#[derive(Debug, thiserror::Error)]
pub enum MetaWriteError {
    /// No meta leader is known, and no candidate named one.
    #[error("the meta group has no known leader")]
    NoLeader,
    /// The meta leader `leader` refused the write, this node included when
    /// it leads.
    #[error("the meta leader {leader} refused the write")]
    Leader {
        leader: NodeId,
        #[source]
        source: GroupEngineError,
    },
    /// The forward to the meta leader `leader` got no answer of the meta
    /// group's.
    #[error("the write forwarded to the meta leader {leader} got no answer")]
    Forward {
        leader: NodeId,
        #[source]
        source: ForwardError,
    },
    /// The meta state machine refused the command.
    #[error("the meta group refused the command")]
    Refused(#[source] ControlError),
    /// A `RegisterNode` names another node than the one the write comes
    /// from: a node registers only itself. Nothing was proposed.
    #[error("a write from node {origin} registers node {named}")]
    OtherNode { origin: NodeId, named: NodeId },
    /// This node runs no meta replica.
    #[error("this node runs no meta replica")]
    Stopped,
}

impl MetaWriteError {
    /// Whether the write may have applied: only a refusal after the command
    /// was proposed, or a forward whose answer was lost, may have.
    pub fn write_effect(&self) -> WriteEffect {
        let effect = match self {
            Self::NoLeader | Self::Refused(_) | Self::OtherNode { .. } | Self::Stopped => {
                return WriteEffect::NotApplied;
            }
            Self::Leader { source, .. } => source.write_effect(),
            Self::Forward { source, .. } => source.write_effect(),
        };
        match effect {
            ursula_runtime::WriteEffect::NotProposed | ursula_runtime::WriteEffect::Refused => {
                WriteEffect::NotApplied
            }
            ursula_runtime::WriteEffect::Unknown => WriteEffect::MayHaveApplied,
        }
    }

    /// The leader a refusal names to redirect to, for one forward hop.
    fn redirect(&self) -> Option<NodeId> {
        match self {
            Self::Leader { source, .. } => source
                .not_leader()
                .and_then(|refusal| refusal.leader.as_ref())
                .map(|leader| NodeId::new(leader.node_id)),
            Self::NoLeader
            | Self::Forward { .. }
            | Self::Refused(_)
            | Self::OtherNode { .. }
            | Self::Stopped => None,
        }
    }

    /// Whether the node asked gave no answer of the meta group's and applied
    /// nothing, so the next candidate may be asked: it was not reached, or
    /// it does not lead.
    fn no_answer(&self) -> bool {
        match self {
            Self::Leader { source, .. } => source.not_leader().is_some(),
            Self::Forward { .. } => self.write_effect() == WriteEffect::NotApplied,
            Self::NoLeader | Self::Refused(_) | Self::OtherNode { .. } | Self::Stopped => false,
        }
    }
}

/// This node's way to the meta group (see the module docs).
#[derive(Debug, Clone)]
pub struct MetaHandle<T> {
    me: NodeId,
    registry: RaftGroupHandleRegistry,
    transport: T,
    /// The meta voters this node claims, the candidates' last resort.
    claimed: VoterSet,
    /// The ids of `raft.peers`.
    peers: BTreeSet<NodeId>,
    /// This run's root, which every confirmed view is checked against.
    root: Option<RootGuard>,
}

impl<T: RecoveryTransport> MetaHandle<T> {
    /// The handle of node `me`, which claims `claim` at genesis.
    pub fn new(
        me: NodeId,
        registry: RaftGroupHandleRegistry,
        transport: T,
        claim: &GenesisClaim,
    ) -> Self {
        Self {
            me,
            registry,
            transport,
            claimed: claim.meta_voters.clone(),
            peers: claim.peers.to_set(),
            root: None,
        }
    }

    /// Checks every view this handle confirms against `root`: a confirmed
    /// view that binds this node id elsewhere supersedes the process.
    pub(crate) fn with_root_guard(mut self, root: RootGuard) -> Self {
        self.root = Some(root);
        self
    }

    /// A watch of this node's placement views.
    pub fn watch(&self) -> PlacementWatch {
        self.registry.placement_watch()
    }

    /// Writes `command` to the meta group through its leader (see the
    /// module docs).
    pub async fn submit(&self, command: ControlCommand) -> Result<ControlApplied, MetaWriteError> {
        let active = self.registry.meta().ok_or(MetaWriteError::Stopped)?;
        let (leader, membership) = {
            let watched = active.core.owner().metrics();
            let metrics = watched.borrow_watched();
            let membership = membership_config(
                &metrics.committed_membership_config,
                MembershipKind::Committed,
            )
            .ok();
            (metrics.current_leader.map(NodeId::new), membership)
        };
        if let Some(leader) = leader {
            return self.forward(&active, leader, command).await;
        }
        let view = self.registry.placement_watch().current();
        let candidates = leader_candidates(
            self.me,
            CandidatesOf::Meta {
                claimed: &self.claimed,
            },
            &view,
            membership.as_ref(),
            &self.peers,
        );
        for candidate in candidates.iter() {
            match self.forward(&active, candidate, command.clone()).await {
                Err(error) if error.no_answer() => {}
                answer => return answer,
            }
        }
        Err(MetaWriteError::NoLeader)
    }

    /// Writes `command` through `leader`, following one redirect.
    async fn forward(
        &self,
        active: &ActiveMeta,
        leader: NodeId,
        command: ControlCommand,
    ) -> Result<ControlApplied, MetaWriteError> {
        let answer = self.write_once(active, leader, command.clone()).await;
        match answer.as_ref().err().and_then(MetaWriteError::redirect) {
            Some(next) if next != leader => self.write_once(active, next, command).await,
            Some(_) | None => answer,
        }
    }

    /// Writes `command` on `leader`: in place when it is this node.
    async fn write_once(
        &self,
        active: &ActiveMeta,
        leader: NodeId,
        command: ControlCommand,
    ) -> Result<ControlApplied, MetaWriteError> {
        let answer = if leader == self.me {
            let answers = active
                .write(
                    vec![RaftCommand::Meta(MetaCommand::Control(command))],
                    WriteOrigin::Local(self.me),
                )
                .await?;
            crate::forward::single_control_answer(answers)
                .map_err(|source| MetaWriteError::Leader { leader, source })?
        } else {
            self.transport
                .forward_control(leader.get(), command)
                .await
                .map_err(|error| match error {
                    MetaForwardError::Leader(source) => MetaWriteError::Leader { leader, source },
                    MetaForwardError::Transport(source) => {
                        MetaWriteError::Forward { leader, source }
                    }
                })?
        };
        answer.map_err(MetaWriteError::Refused)
    }

    /// Asks `leader` for a fresh quorum barrier of `target` over this
    /// node's recovery transport, the one seam every asker of a group's
    /// memberships uses.
    pub(crate) async fn barrier(
        &self,
        leader: NodeId,
        target: RaftTarget,
    ) -> Result<BarrierAnswer, BarrierFailure> {
        self.transport
            .barrier(leader.get(), target)
            .await
            .map_err(Into::into)
    }

    /// Confirms this node's placement view by a meta read barrier (see the
    /// module docs).
    pub async fn confirm_view(&self) -> Result<ConfirmedView, MetaBarrierError> {
        let active = self.registry.meta().ok_or(MetaBarrierError::Stopped)?;
        let leader = active
            .core
            .owner()
            .metrics()
            .borrow_watched()
            .current_leader
            .map(NodeId::new)
            .ok_or(MetaBarrierError::NoLeader)?;
        let answer = if leader == self.me {
            active
                .confirm_recovery_barrier()
                .await
                .map_err(|error| MetaBarrierError::Barrier {
                    leader,
                    failure: error.into(),
                })?
        } else {
            self.transport
                .barrier(leader.get(), RaftTarget::Meta)
                .await
                .map_err(|error| MetaBarrierError::Barrier {
                    leader,
                    failure: error.into(),
                })?
        };
        drop(active);
        let bound = barrier_bound(&answer);
        let mut watch = self.registry.placement_watch();
        let confirmed = crate::rt::time::timeout(CONFIRM_WAIT, async {
            loop {
                match confirm(watch.current(), bound) {
                    // Not applied up to the bound yet: wait for the next view.
                    Err(UnconfirmedView::Empty | UnconfirmedView::Behind { .. }) => {}
                    confirmed => {
                        return confirmed
                            .map_err(|source| MetaBarrierError::Unconfirmed { source });
                    }
                }
                if !watch.changed().await {
                    return Err(MetaBarrierError::Stopped);
                }
            }
        })
        .await
        .map_err(|_elapsed| MetaBarrierError::TimedOut {
            waited: CONFIRM_WAIT,
        })??;
        if let Some(root) = &self.root {
            root.check_view(confirmed.view(), true);
        }
        self.registry
            .placement_publisher()
            .record_confirmation(confirmed.stamp());
        Ok(confirmed)
    }
}

/// The meta log id a barrier bounds the log by: the leader's committed vote
/// (term and leader) and the barrier's index.
pub(crate) fn barrier_bound(answer: &BarrierAnswer) -> MetaLogId {
    MetaLogId::new(RaftLogId {
        term: answer.vote.leader_id.term,
        leader: NodeId::new(answer.vote.leader_id.node_id),
        index: answer.index,
    })
}

/// `view` confirmed at `bound` in the history it applied, by
/// [`ConfirmedView::new`]'s rule (term first): `Behind` until the view
/// applied the bound.
fn confirm(view: Arc<PlacementView>, bound: MetaLogId) -> Result<ConfirmedView, UnconfirmedView> {
    let history = match view.stage {
        ViewStage::Applied { history, .. } => history,
        ViewStage::Empty => return Err(UnconfirmedView::Empty),
    };
    ConfirmedView::new(view, ConfirmedStamp {
        history,
        applied: bound,
    })
}
