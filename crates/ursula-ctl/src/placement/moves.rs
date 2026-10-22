//! `placement move`, `cancel` and `set-move-limit`.
//!
//! A move runs in passes. Each pass reads a view a meta read barrier
//! confirmed and decides every group still pending with the kernel's own
//! [`ControlPlaneState::check`], so the tool holds no copy of a rule:
//!
//! - a group already stable at, or moving to, its target counts as done, so
//!   a rerun finishes an interrupted batch;
//! - a moving group keeps its target unless the move retargets or cancels;
//! - a request the kernel would refuse now and may take later, such as one
//!   over the move limit ([`Requeue::Retry`]), stays queued for the next
//!   pass, and every other refusal is reported for its group.
//!
//! An accepted request is applied to the pass's view at the entry that
//! answered it, so the next check of the pass counts its move against the
//! limit. A write that may have applied (a `504` or a lost answer) is
//! decided again from the next confirmed view, which shows whether it took
//! effect. With `wait`, the batch ends once every group is stable at its
//! target, and each moving group's leader state is printed as it changes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use reqwest::StatusCode;
use ursula_control::ClusterView;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::ControlPlaneState;
use ursula_control::GroupPlacement;
use ursula_control::MoveLimit;
use ursula_control::MoveLimitRequest;
use ursula_control::MoveLimitResponse;
use ursula_control::MoveRequest;
use ursula_control::MoveResponse;
use ursula_control::PlacementApiError;
use ursula_control::PlacementApiReason;
use ursula_control::Requeue;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::PlacementEpoch;
use ursula_proto::placement::VoterSet;
use ursula_proto::placement::WriteEffect;
use ursula_shard::RaftGroupId;

use super::WriteAnswer;
use super::confirmed_status;
use super::post_write;
use super::status::describe_placement;
use super::status::find_leader;
use super::status::join;
use crate::maintenance::Deadline;
use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// What a move asks of each of its groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Intent {
    /// Move to these voters.
    To(VoterSet),
    /// Replace one voter of the group's target by another node.
    Replace(Replacement),
    /// Move a moving group back to its source.
    Cancel,
}

/// `--replace OLD=NEW`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replacement {
    pub old: NodeId,
    pub new: NodeId,
}

/// A replacement that is not `OLD=NEW` with two different node ids.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a replacement is OLD=NEW with two different node ids, such as 3=4, not '{found}'")]
pub struct InvalidReplacement {
    pub found: String,
}

impl FromStr for Replacement {
    type Err = InvalidReplacement;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidReplacement {
            found: value.to_owned(),
        };
        let (old, new) = value.split_once('=').ok_or_else(invalid)?;
        let old: u64 = old.parse().map_err(|_not_a_number| invalid())?;
        let new: u64 = new.parse().map_err(|_not_a_number| invalid())?;
        if old == new {
            return Err(invalid());
        }
        Ok(Self {
            old: NodeId::new(old),
            new: NodeId::new(new),
        })
    }
}

/// Why a move sends no request for a group, or stopped asking.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MoveRefusal {
    /// The meta state machine refuses the request, on the confirmed view or
    /// in its answer.
    #[error("{0}")]
    Kernel(ControlError),
    #[error(
        "group {} is moving to {}: pass --retarget to change its target",
        .group.0,
        join(.to.iter())
    )]
    Moving { group: RaftGroupId, to: VoterSet },
    #[error("node {node} is not a voter of group {}'s target", .group.0)]
    NotAVoter { group: RaftGroupId, node: NodeId },
    #[error("group {} now targets {}: another request changed its placement", .group.0, join(.current.target_voters().iter()))]
    Superseded {
        group: RaftGroupId,
        current: Box<GroupPlacement>,
    },
    /// A refusal of `set-move-limit`, with the node that answered it. A
    /// move never reports one: it plans a group again after a refusal that
    /// is not the meta state machine's, such as no meta leader, and reports
    /// the meta state machine's own as [`Self::Kernel`].
    #[error("node {via} refused ({status}): {}", .refusal.message)]
    Api {
        via: u64,
        status: StatusCode,
        refusal: Box<PlacementApiError>,
    },
    #[error("node {via} answered {status}: {body}")]
    Unexpected {
        via: u64,
        status: StatusCode,
        body: String,
    },
}

/// What a pass does with one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The group is stable at `to` (`stable`), or moving to it.
    Done {
        to: VoterSet,
        stable: bool,
    },
    /// Send this request.
    Submit(MoveRequest),
    /// The meta state machine would refuse the request now and may take it
    /// later: ask again in the next pass.
    Queue(ControlError),
    Refuse(MoveRefusal),
}

/// Decides `intent` for `group` on `state`, a confirmed view. `expect`
/// replaces the placement key the view shows, and a moving group takes
/// another target only with `retarget` or under the `expect` the operator
/// read from it.
pub fn decide(
    state: &ControlPlaneState,
    group: RaftGroupId,
    intent: &Intent,
    expect: Option<PlacementEpoch>,
    retarget: bool,
) -> Decision {
    let cluster = match state {
        ControlPlaneState::Bootstrapped(cluster) => cluster,
        ControlPlaneState::Forming { .. } => {
            return Decision::Queue(ControlError::NotBootstrapped);
        }
        ControlPlaneState::FormationCut => {
            return Decision::Refuse(MoveRefusal::Kernel(ControlError::FormationCut));
        }
    };
    let Some(placement) = cluster.group(group) else {
        return Decision::Refuse(MoveRefusal::Kernel(ControlError::UnknownGroup { group }));
    };
    let moving = placement.moving();
    let to = match intent {
        Intent::To(to) => to.clone(),
        Intent::Replace(replacement) => match replaced(placement, *replacement) {
            Some(to) => to,
            None => {
                return Decision::Refuse(MoveRefusal::NotAVoter {
                    group,
                    node: replacement.old,
                });
            }
        },
        Intent::Cancel => match moving {
            Some(moving) => moving.from.clone(),
            None => {
                return Decision::Refuse(MoveRefusal::Kernel(ControlError::NotMoving { group }));
            }
        },
    };
    match moving {
        None if placement.target_voters() == &to => {
            return Decision::Done { to, stable: true };
        }
        Some(moving) if moving.to == to => return Decision::Done { to, stable: false },
        Some(moving) if !retarget && expect.is_none() && *intent != Intent::Cancel => {
            return Decision::Refuse(MoveRefusal::Moving {
                group,
                to: moving.to.clone(),
            });
        }
        None | Some(_) => {}
    }
    let request = MoveRequest {
        expected: expect.unwrap_or_else(|| placement.since()),
        to,
    };
    match state.check(&request_move(group, &request, 0)) {
        Ok(()) => Decision::Submit(request),
        Err(error) => match error.requeue() {
            Requeue::Retry => Decision::Queue(error),
            Requeue::ConfirmView | Requeue::Permanent => {
                Decision::Refuse(MoveRefusal::Kernel(error))
            }
        },
    }
}

/// The voters of `placement`'s target with `replacement.old` replaced, the
/// target itself when it already holds `new` and not `old`, and `None` when
/// it holds neither.
pub fn replaced(placement: &GroupPlacement, replacement: Replacement) -> Option<VoterSet> {
    let target = placement.target_voters();
    if target.contains(replacement.old) {
        let rest = target.iter().filter(|node| *node != replacement.old);
        Some(VoterSet::from_first(replacement.new, rest))
    } else if target.contains(replacement.new) {
        Some(target.clone())
    } else {
        None
    }
}

fn request_move(group: RaftGroupId, request: &MoveRequest, now_ms: u64) -> ControlCommand {
    ControlCommand::RequestMove {
        group,
        expected: request.expected,
        to: request.to.clone(),
        now_ms,
    }
}

/// What the batch does after a node answered a move request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answered {
    Accepted(PlacementEpoch),
    /// Decide the group again from the next confirmed view.
    Pending(Pending),
    Refuse(MoveRefusal),
}

/// Why a group waits for the next pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pending {
    /// The meta state machine refuses the request for now.
    Queued(ControlError),
    /// The write may have applied: the next confirmed view shows whether.
    MayHaveApplied,
    /// The view the request was planned from is stale, or the meta group
    /// could not answer.
    Replan(PlacementApiReason),
}

/// Classifies `via`'s answer to a move request. A refusal that asks for a
/// newer view plans the group again, unless the operator named the
/// placement key (`expect_given`): a stale `--expect` is the answer.
pub fn answered(via: u64, answer: WriteAnswer<MoveResponse>, expect_given: bool) -> Answered {
    match answer {
        WriteAnswer::Applied(MoveResponse { since }) => Answered::Accepted(since),
        WriteAnswer::Lost(error) => {
            tracing::warn!(node_id = via, %error, "placement: no answer to a move request");
            Answered::Pending(Pending::MayHaveApplied)
        }
        WriteAnswer::Unexpected { status, body } => {
            Answered::Refuse(MoveRefusal::Unexpected { via, status, body })
        }
        WriteAnswer::Refused { status: _, refusal } => {
            if refusal.write_effect == WriteEffect::MayHaveApplied {
                return Answered::Pending(Pending::MayHaveApplied);
            }
            match &refusal.reason {
                PlacementApiReason::Refused(error) => match error.requeue() {
                    Requeue::Retry => Answered::Pending(Pending::Queued(error.clone())),
                    Requeue::ConfirmView if !expect_given => {
                        Answered::Pending(Pending::Replan(refusal.reason.clone()))
                    }
                    Requeue::ConfirmView | Requeue::Permanent => {
                        Answered::Refuse(MoveRefusal::Kernel(error.clone()))
                    }
                },
                PlacementApiReason::NoLeader
                | PlacementApiReason::ForwardFailed { .. }
                | PlacementApiReason::LeaderRefused { .. }
                | PlacementApiReason::Stopped
                | PlacementApiReason::Unconfirmed => {
                    Answered::Pending(Pending::Replan(refusal.reason.clone()))
                }
            }
        }
    }
}

/// How `move` and `cancel` run.
#[derive(Debug, Clone)]
pub struct MoveOptions {
    pub groups: Vec<RaftGroupId>,
    pub intent: Intent,
    /// The placement key to name, in place of the view's: one group only.
    pub expect: Option<PlacementEpoch>,
    /// Let a moving group take the new target. Naming its key with
    /// `expect` does too.
    pub retarget: bool,
    /// Wait until every group is stable at its target.
    pub wait: bool,
    /// Bounds the whole command: queued groups and, with `wait`, the moves.
    pub timeout: Duration,
    pub poll_interval: Duration,
}

/// How one group of a move ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupEnd {
    /// The meta group took the request: the group moves to `to` from
    /// placement key `since`.
    Accepted {
        to: VoterSet,
        since: PlacementEpoch,
    },
    /// Nothing was sent: the group was already stable at or moving to `to`.
    Already {
        to: VoterSet,
        stable: bool,
    },
    /// `wait`: the group is stable at `to`.
    Converged {
        to: VoterSet,
    },
    Refused(MoveRefusal),
    /// The timeout passed while the group waited.
    TimedOut {
        waiting: Option<Pending>,
    },
}

impl GroupEnd {
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Refused(_) | Self::TimedOut { .. })
    }
}

/// Where one group of a running batch stands.
#[derive(Debug, Clone)]
enum Stage {
    Pending(Option<Pending>),
    /// Sent or found at `to`. `wait` waits for it to be stable there.
    Moving {
        end: GroupEnd,
        to: VoterSet,
    },
    Ended(GroupEnd),
}

/// Runs a move or cancel batch and returns how each group ended, writing
/// progress to `out`.
pub async fn run_moves(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    options: &MoveOptions,
    out: &mut impl Write,
) -> Result<BTreeMap<RaftGroupId, GroupEnd>> {
    if options.expect.is_some() && options.groups.len() != 1 {
        anyhow::bail!("--expect names the placement key of one group: pass exactly one --group");
    }
    let deadline = Deadline::after(options.timeout);
    let mut stages: BTreeMap<RaftGroupId, Stage> = options
        .groups
        .iter()
        .map(|group| (*group, Stage::Pending(None)))
        .collect();
    let mut progress: BTreeMap<RaftGroupId, String> = BTreeMap::new();
    loop {
        if let Some((via, status)) = confirmed_status(client, nodes).await {
            let mut state = match status.view {
                ClusterView::Bootstrapped(cluster) => ControlPlaneState::Bootstrapped(cluster),
                ClusterView::FormationCut => ControlPlaneState::FormationCut,
                forming @ (ClusterView::MetaGenesis { .. }
                | ClusterView::MetaCatchingUp
                | ClusterView::Registering { .. }) => {
                    writeln!(out, "node {}: the view is {:?}", via.id, forming.stage())?;
                    ControlPlaneState::default()
                }
            };
            for (group, stage) in &mut stages {
                pass_group(client, via, &mut state, *group, stage, options, out).await?;
            }
            if options.wait {
                for (group, stage) in &mut stages {
                    watch_group(client, nodes, &state, *group, stage, &mut progress, out).await?;
                }
            }
        } else {
            writeln!(out, "no node answered a confirmed placement view")?;
        }
        let open = stages.values().any(|stage| match stage {
            Stage::Pending(_) => true,
            Stage::Moving { .. } => options.wait,
            Stage::Ended(_) => false,
        });
        if !open {
            break;
        }
        if deadline.is_reached() {
            for stage in stages.values_mut() {
                let waiting = match stage {
                    Stage::Pending(pending) => pending.clone(),
                    Stage::Moving { .. } => None,
                    Stage::Ended(_) => continue,
                };
                *stage = Stage::Ended(GroupEnd::TimedOut { waiting });
            }
            break;
        }
        tokio::time::sleep(options.poll_interval).await;
    }
    Ok(stages
        .into_iter()
        .map(|(group, stage)| {
            let end = match stage {
                Stage::Pending(waiting) => GroupEnd::TimedOut { waiting },
                Stage::Moving { end, .. } | Stage::Ended(end) => end,
            };
            (group, end)
        })
        .collect())
}

/// Decides and, when it must, sends one pending group of a pass.
async fn pass_group(
    client: &MetricsClient,
    via: &NodeInfo,
    state: &mut ControlPlaneState,
    group: RaftGroupId,
    stage: &mut Stage,
    options: &MoveOptions,
    out: &mut impl Write,
) -> Result<()> {
    let Stage::Pending(waiting) = stage else {
        return Ok(());
    };
    let after_lost = *waiting == Some(Pending::MayHaveApplied);
    let decision = decide(
        state,
        group,
        &options.intent,
        options.expect,
        options.retarget,
    );
    if after_lost {
        let took = matches!(decision, Decision::Done { .. });
        writeln!(
            out,
            "group {}: the confirmed view shows the earlier request {}",
            group.0,
            if took {
                "took effect"
            } else {
                "did not take effect"
            }
        )?;
    }
    match decision {
        Decision::Done { to, stable } => {
            if !after_lost {
                let at = if stable { "stable at" } else { "moving to" };
                writeln!(out, "group {}: already {at} {}", group.0, join(to.iter()))?;
            }
            let end = GroupEnd::Already {
                to: to.clone(),
                stable,
            };
            *stage = Stage::Moving { end, to };
        }
        Decision::Queue(error) => {
            if *waiting != Some(Pending::Queued(error.clone())) {
                writeln!(out, "group {}: queued, {error}", group.0)?;
            }
            *stage = Stage::Pending(Some(Pending::Queued(error)));
        }
        Decision::Refuse(refusal) => *stage = refused(group, refusal, out)?,
        Decision::Submit(request) => {
            warn_decommissioning(state, group, &options.intent, out)?;
            let path = format!("/__ursula/placement/groups/{}/move", group.0);
            let answer = post_write(client, via, &path, &request).await;
            match answered(via.id, answer, options.expect.is_some()) {
                Answered::Accepted(since) => {
                    writeln!(
                        out,
                        "group {}: moving to {}, since {since}",
                        group.0,
                        join(request.to.iter())
                    )?;
                    // `now_ms` is display only, and the pass's view is never shown.
                    let applied = state.apply(since.log_id(), request_move(group, &request, 0));
                    if let Err(error) = applied {
                        tracing::warn!(group = group.0, %error, "placement: the local view did not take the accepted move");
                    }
                    let end = GroupEnd::Accepted {
                        to: request.to.clone(),
                        since,
                    };
                    *stage = Stage::Moving {
                        end,
                        to: request.to,
                    };
                }
                Answered::Pending(pending) => {
                    let why = match &pending {
                        Pending::Queued(error) => format!("queued, {error}"),
                        Pending::MayHaveApplied => {
                            "the request may have applied, reading a confirmed view".to_owned()
                        }
                        Pending::Replan(reason) => format!("planning again ({reason:?})"),
                    };
                    writeln!(out, "group {}: {why}", group.0)?;
                    *stage = Stage::Pending(Some(pending));
                }
                Answered::Refuse(refusal) => *stage = refused(group, refusal, out)?,
            }
        }
    }
    Ok(())
}

/// Prints a group's refusal, with the placement a stale key met, and ends
/// the group.
fn refused(group: RaftGroupId, refusal: MoveRefusal, out: &mut impl Write) -> Result<Stage> {
    writeln!(out, "group {}: refused, {refusal}", group.0)?;
    if let MoveRefusal::Kernel(ControlError::StalePlacement { current, .. }) = &refusal {
        writeln!(out, "  placement: {}", describe_placement(current))?;
    }
    Ok(Stage::Ended(GroupEnd::Refused(refusal)))
}

/// `wait`: ends a moving group once the view shows it stable at its target,
/// or targeting other voters, and prints its leader's state as it changes.
async fn watch_group(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    state: &ControlPlaneState,
    group: RaftGroupId,
    stage: &mut Stage,
    progress: &mut BTreeMap<RaftGroupId, String>,
    out: &mut impl Write,
) -> Result<()> {
    let Stage::Moving { to, .. } = stage else {
        return Ok(());
    };
    let Some(placement) = state.cluster().and_then(|cluster| cluster.group(group)) else {
        return Ok(());
    };
    if placement.target_voters() != &*to {
        let refusal = MoveRefusal::Superseded {
            group,
            current: Box::new(placement.clone()),
        };
        writeln!(out, "group {}: {refusal}", group.0)?;
        *stage = Stage::Ended(GroupEnd::Refused(refusal));
        return Ok(());
    }
    if placement.moving().is_none() {
        writeln!(out, "group {}: stable at {}", group.0, join(to.iter()))?;
        *stage = Stage::Ended(GroupEnd::Converged { to: to.clone() });
        return Ok(());
    }
    let line = find_leader(client, nodes, group, placement)
        .await
        .progress();
    if progress.get(&group) != Some(&line) {
        writeln!(out, "group {}: {line}", group.0)?;
        progress.insert(group, line);
    }
    Ok(())
}

/// A cancel moves the group back onto its source: a decommissioning node
/// among it gets the group back, and the decommission moves it away again
/// unless the node is uncordoned.
fn warn_decommissioning(
    state: &ControlPlaneState,
    group: RaftGroupId,
    intent: &Intent,
    out: &mut impl Write,
) -> Result<()> {
    if *intent != Intent::Cancel {
        return Ok(());
    }
    let Some(cluster) = state.cluster() else {
        return Ok(());
    };
    let Some(moving) = cluster.group(group).and_then(GroupPlacement::moving) else {
        return Ok(());
    };
    let leaving: BTreeSet<NodeId> = moving
        .from
        .iter()
        .filter(|node| {
            cluster
                .node(*node)
                .is_some_and(|record| record.lifecycle == NodeLifecycle::Decommissioning)
        })
        .collect();
    if !leaving.is_empty() {
        writeln!(
            out,
            "group {}: warning, nodes {} are decommissioning: the decommission moves the group \
             away again unless they are uncordoned",
            group.0,
            join(leaving.iter())
        )?;
    }
    Ok(())
}

/// How `set-move-limit` ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveLimitEnd {
    /// The meta group answered that it applied this limit.
    Applied(MoveLimit),
    /// The write may have applied (a `504` or a lost answer), and the next
    /// confirmed view shows the limit `current`: the request took effect
    /// when it is the limit asked for.
    Resolved {
        asked: MoveLimit,
        current: MoveLimit,
    },
    Refused(MoveRefusal),
}

impl MoveLimitEnd {
    /// Whether the move limit is the one asked for.
    pub fn took_effect(&self) -> bool {
        match self {
            Self::Applied(_) => true,
            Self::Resolved { asked, current } => asked == current,
            Self::Refused(_) => false,
        }
    }
}

/// `set-move-limit`: sets the move limit through the node that confirmed a
/// view. When the write may have applied, it reads the next confirmed view
/// and reports the limit it shows. Setting a limit is idempotent, so a
/// request that did not take effect can be sent again.
pub async fn set_move_limit(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    limit: MoveLimit,
) -> Result<MoveLimitEnd> {
    let (via, _status) = confirmed_status(client, nodes)
        .await
        .ok_or_else(|| anyhow::anyhow!("no node answered a confirmed placement view"))?;
    let request = MoveLimitRequest { limit };
    match post_write(client, via, "/__ursula/placement/move-limit", &request).await {
        WriteAnswer::Applied(MoveLimitResponse { limit }) => Ok(MoveLimitEnd::Applied(limit)),
        WriteAnswer::Refused { status, refusal } => {
            if refusal.write_effect == WriteEffect::MayHaveApplied {
                tracing::warn!(node_id = via.id, %status, message = %refusal.message, "placement: the move limit may have been set");
                return confirmed_limit(client, nodes, limit).await;
            }
            Ok(MoveLimitEnd::Refused(MoveRefusal::Api {
                via: via.id,
                status,
                refusal,
            }))
        }
        WriteAnswer::Unexpected { status, body } => {
            Ok(MoveLimitEnd::Refused(MoveRefusal::Unexpected {
                via: via.id,
                status,
                body,
            }))
        }
        WriteAnswer::Lost(error) => {
            tracing::warn!(node_id = via.id, %error, "placement: no answer to the move limit");
            confirmed_limit(client, nodes, limit).await
        }
    }
}

/// The move limit a confirmed view shows, against the limit `asked` for.
async fn confirmed_limit(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    asked: MoveLimit,
) -> Result<MoveLimitEnd> {
    let may_have = "the move limit may have been set";
    let (via, status) = confirmed_status(client, nodes).await.ok_or_else(|| {
        anyhow::anyhow!(
            "{may_have}, and no node answered a confirmed placement view to tell: read \
             `placement status --confirmed` to learn whether"
        )
    })?;
    match status.view {
        ClusterView::Bootstrapped(cluster) => Ok(MoveLimitEnd::Resolved {
            asked,
            current: cluster.params().max_concurrent_moves,
        }),
        other => anyhow::bail!(
            "{may_have}, and node {}'s confirmed view is {:?}, with no move limit",
            via.id,
            other.stage()
        ),
    }
}

#[cfg(test)]
mod tests;
