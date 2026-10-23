//! Owner-runtime mailbox for protocol, read and administrative Raft calls.
//! Only the mailbox worker owns a raw handle; remote callers keep metrics and
//! a sender. Jobs are polled concurrently so a waiting write cannot prevent
//! the AppendEntries or vote that lets it complete.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use openraft::ChangeMembers;
use openraft::EmptyNode;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::error::ClientWriteError;
use openraft::error::Fatal;
#[cfg(all(test, not(madsim)))]
use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::metrics::RaftDataMetrics;
use openraft::metrics::RaftServerMetrics;
use openraft::raft::ClientWriteResponse;
use openraft::raft::Precondition;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::rt::WatchReceiver;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::vote::RaftLeaderId;
use ursula_runtime::GroupEngineError;
use ursula_runtime::NodeFence;

use crate::leadership::GroupLeadership;
use crate::leadership::LeaderContact;
use crate::leadership::named_leader;
use crate::openraft_work::TransferAnswer;
use crate::openraft_work::TransferOutcome;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::rt::sync::watch;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::SnapshotData;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaSnapshot;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

type Job<SM> = Box<dyn FnOnce(Raft<C, SM>) -> BoxFuture<'static, ()> + Send>;

struct Mailbox<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> {
    sender: mpsc::UnboundedSender<Job<SM>>,
    task: crate::rt::task::JoinHandle<()>,
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> Drop for Mailbox<SM> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A cloneable command endpoint, with no cross-thread raw Raft handle.
/// Generic over the state machine, so every plane's replica uses one
/// mailbox. The default is the data plane's.
pub struct OwnerRaftHandle<
    SM: RaftStateMachine<C, SnapshotData = SnapshotData> = RaftGroupStateMachine,
> {
    mailbox: Arc<Mailbox<SM>>,
    metrics: WatchReceiverOf<C, RaftMetrics<C>>,
    data_metrics: WatchReceiverOf<C, RaftDataMetrics<C>>,
    server_metrics: WatchReceiverOf<C, RaftServerMetrics<C>>,
    handoff_ack_window: Duration,
    transfer_deadline: Duration,
    /// The node fence the one leader predicate reads.
    fence: NodeFence,
    /// The group's OpenRaft leader lease, which the one leader predicate
    /// reads too ([`crate::leadership::leader_lease`]).
    lease: Duration,
    /// The replica's last contact with its leader, which the one leader
    /// predicate reads for a follower ([`crate::leadership::named_leader`]).
    contact: LeaderContact,
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> Clone for OwnerRaftHandle<SM> {
    fn clone(&self) -> Self {
        Self {
            mailbox: self.mailbox.clone(),
            metrics: self.metrics.clone(),
            data_metrics: self.data_metrics.clone(),
            server_metrics: self.server_metrics.clone(),
            handoff_ack_window: self.handoff_ack_window,
            transfer_deadline: self.transfer_deadline,
            fence: self.fence.clone(),
            lease: self.lease,
            contact: self.contact.clone(),
        }
    }
}

/// How many maximum election timeouts a submitted leadership transfer may
/// take before the leader abandons it. The target waits at most one minimum
/// election timeout for its log, then campaigns.
const TRANSFER_DEADLINE_ELECTION_TIMEOUTS: u64 = 2;

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> std::fmt::Debug for OwnerRaftHandle<SM> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerRaftHandle").finish_non_exhaustive()
    }
}

impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> OwnerRaftHandle<SM> {
    pub(crate) fn new(raft: Raft<C, SM>, fence: NodeFence) -> Self {
        let metrics = raft.metrics();
        let data_metrics = raft.data_metrics();
        let server_metrics = raft.server_metrics();
        let handoff_ack_window = Duration::from_millis(raft.config().election_timeout_min);
        let lease = crate::leadership::leader_lease(raft.config());
        let transfer_deadline = Duration::from_millis(
            raft.config()
                .election_timeout_max
                .saturating_mul(TRANSFER_DEADLINE_ELECTION_TIMEOUTS),
        );
        let (sender, mut receiver) = mpsc::unbounded_channel::<Job<SM>>();
        let task = crate::rt::spawn(async move {
            let mut jobs = FuturesUnordered::new();
            loop {
                crate::rt::select! {
                    biased;
                    _ = jobs.next(), if !jobs.is_empty() => {},
                    job = receiver.recv() => match job {
                        Some(job) => jobs.push(job(raft.clone())),
                        None => break,
                    },
                }
            }
            while jobs.next().await.is_some() {}
        });
        Self {
            mailbox: Arc::new(Mailbox { sender, task }),
            metrics,
            data_metrics,
            server_metrics,
            handoff_ack_window,
            transfer_deadline,
            fence,
            lease,
            contact: LeaderContact::default(),
        }
    }

    /// The node fence this replica's leadership reads.
    pub fn fence(&self) -> &NodeFence {
        &self.fence
    }

    /// The replica's last contact with its leader, which its leadership
    /// reads too.
    pub fn contact(&self) -> &LeaderContact {
        &self.contact
    }

    /// How recently a handoff target must have acknowledged this leader:
    /// a follower that has not answered for a minimum election timeout may
    /// be down, and a transfer to it cannot complete.
    pub(crate) fn handoff_ack_window(&self) -> Duration {
        self.handoff_ack_window
    }

    /// The group's OpenRaft leader lease ([`crate::leadership::leader_lease`]).
    pub(crate) fn lease(&self) -> Duration {
        self.lease
    }

    /// How long after this leader submitted a leadership transfer it may
    /// still lead at the transfer's vote: the transfer deadline, after which
    /// it abandons the transfer and steps down
    /// ([`Self::abandon_stalled_transfer`]), and one minimum election timeout
    /// for a refused step-down to be retried. A node that still leads the
    /// group after that long knows the transfer did not move its leadership.
    pub fn handoff_settle_time(&self) -> Duration {
        self.transfer_deadline
            .saturating_add(self.handoff_ack_window)
    }

    /// Abandons the leadership transfer to `target` this leader submitted at
    /// `vote` unless it moves the leadership: at once when the target
    /// answers that it does not take it, or the transfer never left this
    /// node ([`TransferOutcome::Refused`] in `answers`, subscribed before
    /// the transfer started), within one minimum election timeout of a
    /// target's answer that it does, within one leader lease of the
    /// transfer when the target already holds
    /// another vote ([`TransferOutcome::Superseded`]) or its answer is lost,
    /// which leaves a newer election or the target's own campaign that
    /// long, and otherwise once the transfer deadline passed
    /// ([`transfer_end`]).
    ///
    /// OpenRaft never abandons a transfer: until it sees a higher vote, the
    /// leader forwards every write to `target`, sends no heartbeats, never
    /// campaigns, and refuses a candidate with a shorter log without adopting
    /// its term. Its `elect` keeps that leader state behind a new candidacy.
    /// So the leader steps down as a follower would, which drops the leader
    /// state, then campaigns with a pre-vote round, shed or not
    /// ([`step_down_then_elect`]), so it never raises the term past a leader
    /// a quorum already follows. A voter that handled the transfer's
    /// broadcast dropped its lease of this leader and grants the round at
    /// once. A target that refused the transfer, or never got it, never
    /// handed it to its Raft, so it keeps that lease and refuses the round
    /// until the lease lapses, one lease after this leader's last
    /// heartbeat. So the leader repeats
    /// the round every minimum election timeout until a leader is known
    /// ([`lead_again`]), whatever its election switch says: with a voter
    /// that took the broadcast it leads again within a round, and otherwise
    /// once the target's lease lapsed. A refused step-down is retried every
    /// minimum election timeout until the leadership or the vote changes.
    pub(crate) fn abandon_stalled_transfer(
        &self,
        vote: UrsulaVote,
        target: u64,
        answers: watch::Receiver<Option<TransferAnswer>>,
    ) {
        let deadline = self.transfer_deadline;
        let retry = self.handoff_ack_window;
        let lease = self.lease;
        let job: Job<SM> = Box::new(move |raft| {
            Box::pin(async move {
                let bounds = TransferBounds {
                    deadline,
                    retry,
                    lease,
                };
                let end = transfer_end(&raft, vote, target, bounds, answers).await;
                if end == TransferEnd::Moved {
                    return;
                }
                loop {
                    match step_down_then_elect(&raft, vote, target, Candidacy::PreVote).await {
                        Ok(StepDown::Campaigned) => {
                            tracing::warn!(
                                node_id = raft.metrics().borrow_watched().id,
                                target,
                                %vote,
                                ?end,
                                "leadership transfer did not complete; stepped down and \
                                 campaigned"
                            );
                            lead_again(&raft, vote, target, retry, deadline).await;
                            return;
                        }
                        Ok(StepDown::Refused { by }) => {
                            tracing::debug!(
                                target,
                                %vote,
                                refused_by = %by,
                                "step-down from a stalled leadership transfer refused; retrying"
                            );
                        }
                        Ok(StepDown::LogAdvanced) => {
                            tracing::debug!(
                                target,
                                %vote,
                                "step-down from a stalled leadership transfer refused for an \
                                 append after it read the log; retrying"
                            );
                        }
                        Err(StepDownError::Campaign(error)) => {
                            tracing::warn!(
                                %error,
                                target,
                                %vote,
                                "stepped down from a stalled leadership transfer, but the \
                                 campaign after it did not start"
                            );
                            return;
                        }
                        Err(error) => {
                            tracing::warn!(
                                ?error,
                                target,
                                %vote,
                                "could not step down from a stalled leadership transfer"
                            );
                            return;
                        }
                    }
                    if settles_within(&raft, vote, retry).await {
                        return;
                    }
                }
            })
        });
        if self.mailbox.sender.send(job).is_err() {
            tracing::debug!("owner Raft stopped before bounding a leadership transfer");
        }
    }

    /// Steps this replica down from the leadership it holds, without
    /// campaigning ([`step_down`]). The vote it grants itself names the
    /// voter that matched the most of its log ([`step_down_target`]), which
    /// still has to win its own election. An append that lands between the
    /// step-down reading the last log id and its vote gets the vote refused
    /// ([`StepDownVote::LogAdvanced`]), so it votes again, up to
    /// [`STEP_DOWN_ATTEMPTS`] times, until the vote is granted or the
    /// leadership moves. `None` when the replica does not lead, or is its
    /// group's only voter, which no other voter could take over from.
    pub(crate) async fn step_down_leadership(&self) -> Option<Result<StepDownVote, StepDownError>> {
        self.call(move |raft| async move {
            let mut attempts = 0_usize;
            loop {
                let metrics = raft.metrics().borrow_watched().clone();
                if metrics.current_leader != Some(metrics.id) {
                    return None;
                }
                let target = step_down_target(&metrics)?;
                attempts = attempts.saturating_add(1);
                match step_down(&raft, metrics.vote, target).await {
                    Ok(StepDownVote::LogAdvanced) if attempts < STEP_DOWN_ATTEMPTS => {}
                    outcome => return Some(outcome),
                }
            }
        })
        .await
        .unwrap_or_else(|stopped| Some(Err(StepDownError::Vote(RaftError::Fatal(stopped)))))
    }

    /// Starts a new term with this leader's own campaign. It steps down
    /// first, to `target`, as [`Self::abandon_stalled_transfer`] does, but
    /// campaigns without a pre-vote round ([`step_down_and_campaign`]).
    /// `target` must be another node. A
    /// single-voter group may name a node outside its membership, which
    /// never answers, so the leader wins its campaign alone.
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn step_down_and_campaign(&self, target: u64) -> Result<StepDown, StepDownError> {
        let vote = self.metrics.borrow_watched().vote;
        self.call(move |raft| async move { step_down_and_campaign(&raft, vote, target).await })
            .await
            .map_err(|stopped| StepDownError::Vote(RaftError::Fatal(stopped)))?
    }

    pub(crate) async fn call<F, Fut, R>(&self, operation: F) -> Result<R, Fatal<C>>
    where
        F: FnOnce(Raft<C, SM>) -> Fut + Send + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let (mut reply, response) = oneshot::channel();
        self.mailbox
            .sender
            .send(Box::new(move |raft| {
                Box::pin(async move {
                    crate::rt::select! {
                        biased;
                        _ = reply.closed() => {},
                        result = operation(raft) => {
                            if reply.send(result).is_err() {
                                tracing::trace!("owner Raft caller stopped waiting");
                            }
                        },
                    }
                })
            }))
            .map_err(|_closed| Fatal::Stopped)?;
        response.await.map_err(|_closed| Fatal::Stopped)
    }

    pub fn metrics(&self) -> WatchReceiverOf<C, RaftMetrics<C>> {
        self.metrics.clone()
    }

    /// This replica's view of the group's leadership, from one borrow of
    /// its Raft metrics, without a trip through the group's core.
    pub fn leadership(&self) -> GroupLeadership {
        GroupLeadership::of(
            &self.metrics.borrow_watched(),
            &self.fence,
            self.lease,
            &self.contact,
        )
    }

    /// OpenRaft's data metrics, which carry the full last log id. Its
    /// `RaftMetrics` report only the last log index.
    pub(crate) fn data_metrics(&self) -> WatchReceiverOf<C, RaftDataMetrics<C>> {
        self.data_metrics.clone()
    }

    /// OpenRaft's server metrics, which change only with the server state
    /// and the leader, so a follower's driver waits on them.
    pub fn server_metrics(&self) -> WatchReceiverOf<C, RaftServerMetrics<C>> {
        self.server_metrics.clone()
    }

    pub fn wait(&self, timeout: Option<Duration>) -> openraft::metrics::Wait<C> {
        openraft::metrics::Wait {
            timeout,
            rx: self.metrics(),
        }
    }

    /// Whether this replica leads, by the one leader predicate
    /// ([`GroupLeadership::of`]), which reads the node fence.
    pub fn is_leader(&self) -> bool {
        self.leadership().leads()
    }

    /// The leader this replica names ([`crate::leadership::named_leader`]):
    /// itself, or a leader it heard from recently.
    pub fn current_leader(&self) -> Option<u64> {
        named_leader(&self.metrics.borrow_watched(), &self.contact)
    }

    pub fn trigger(&self) -> OwnerTrigger<SM> {
        OwnerTrigger(self.clone())
    }

    pub(crate) fn elect(&self, enabled: impl FnOnce() -> bool + Send + 'static) {
        if self
            .mailbox
            .sender
            .send(Box::new(move |raft| {
                // Set before returning the job future, preserving mailbox order.
                raft.runtime_config().elect(enabled());
                Box::pin(async {})
            }))
            .is_err()
        {
            tracing::debug!("owner Raft stopped before election policy update");
        }
    }

    pub async fn append_entries(
        &self,
        request: UrsulaAppendEntriesRequest,
    ) -> Result<UrsulaAppendEntriesResponse, RaftError<C>> {
        self.call(move |raft| async move { raft.append_entries(request).await })
            .await?
    }
    pub async fn vote(
        &self,
        request: UrsulaVoteRequest,
    ) -> Result<UrsulaVoteResponse, RaftError<C>> {
        self.call(move |raft| async move { raft.vote(request).await })
            .await?
    }
    pub async fn install_full_snapshot(
        &self,
        vote: UrsulaVote,
        snapshot: UrsulaSnapshot,
    ) -> Result<SnapshotResponse<C>, Fatal<C>> {
        self.call(move |raft| async move { raft.install_full_snapshot(vote, snapshot).await })
            .await?
    }
    pub(crate) async fn handle_transfer_leader(
        &self,
        request: TransferLeaderRequest<C>,
    ) -> Result<openraft::raft::TransferLeaderResponse<C>, Fatal<C>> {
        self.call(move |raft| async move { raft.handle_transfer_leader(request).await })
            .await?
    }
    pub async fn get_snapshot(&self) -> Result<Option<UrsulaSnapshot>, RaftError<C>> {
        self.call(move |raft| async move { raft.get_snapshot().await })
            .await?
    }
    /// Proposes the membership change `members` only while every one of
    /// `preconditions` holds (OpenRaft's `change_membership_if`), once this
    /// leader's first quorum acknowledgement arrived, as every other
    /// proposal waits for it. A voter change is a joint and a uniform entry
    /// from one future: the preconditions guard the first, and OpenRaft
    /// guards the second with the joint entry's log id and the caller's
    /// committed leader id. Dropping the returned future drops only the
    /// wait for the answer: the change is queued and runs under its guard.
    pub async fn change_membership_if(
        &self,
        members: ChangeMembers<u64, EmptyNode>,
        retain: bool,
        preconditions: Vec<Precondition<C>>,
    ) -> Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>> {
        crate::forward::await_quorum_ack_on(self.metrics(), self.handoff_ack_window).await;
        self.call(move |raft| async move {
            raft.change_membership_if(members, retain, preconditions)
                .await
        })
        .await?
    }
    /// Initializes the group with the voters `members`, for a unit test
    /// that runs no bootstrap driver.
    #[cfg(all(test, not(madsim)))]
    pub(crate) async fn initialize(
        &self,
        members: std::collections::BTreeSet<u64>,
    ) -> Result<(), RaftError<C, InitializeError<C>>> {
        self.call(move |raft| async move { raft.initialize(members).await })
            .await?
    }
    /// Update the timer on its owner and wait for submission.
    pub async fn set_tick(&self, enabled: bool) -> Result<(), Fatal<C>> {
        self.call(move |raft| async move {
            raft.runtime_config().tick(enabled);
        })
        .await
    }

    /// Submit a read-only Raft-state callback on the owner without waiting
    /// for the callback itself. Useful for queue barriers and diagnostics.
    pub async fn external_request<F>(&self, request: F) -> Result<(), Fatal<C>>
    where F: FnOnce(&openraft::RaftState<C>) + Send + 'static {
        self.call(move |raft| async move { raft.external_request(request).await })
            .await?
    }

    pub async fn with_raft_state<F, R>(&self, f: F) -> Result<R, Fatal<C>>
    where
        F: FnOnce(&openraft::RaftState<C>) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.call(move |raft| async move { raft.with_raft_state(f).await })
            .await?
    }
    pub async fn with_state_machine<F, R>(&self, f: F) -> Result<R, Fatal<C>>
    where
        F: FnOnce(&mut SM) -> BoxFuture<'_, R> + Send + 'static,
        R: Send + 'static,
    {
        self.call(move |raft| async move { raft.with_state_machine(f).await })
            .await?
    }
    pub async fn shutdown(&self) -> Result<(), GroupEngineError> {
        self.call(move |raft| async move { raft.shutdown().await.map_err(owner_stopped) })
            .await
            .map_err(owner_stopped)?
    }
}

/// The vote a leader that is stuck in a transfer to `target` at `vote` grants
/// itself to step down: a vote for `target` in the next term, at the leader's
/// own last log id, so its log check passes. It is marked a leadership
/// transfer, which this leader authorizes, so its own leader lease cannot
/// refuse it. Granting it drops the leader state as for any follower; the
/// target's own vote request in that term is still judged by its log.
pub(crate) fn transfer_step_down(
    vote: UrsulaVote,
    target: u64,
    last_log_id: Option<openraft::alias::LogIdOf<C>>,
) -> UrsulaVoteRequest {
    let mut request = UrsulaVoteRequest::new(
        UrsulaVote::new(vote.leader_id().term().saturating_add(1), target),
        last_log_id,
    );
    request.leadership_transfer = true;
    request
}

/// How often [`OwnerRaftHandle::step_down_leadership`] votes when an append
/// keeps landing between its read of the last log id and its vote.
const STEP_DOWN_ATTEMPTS: usize = 8;

/// The voter a leader steps down to: the other voter that matched the most
/// of its log, the lowest id among equals, so the vote names the voter most
/// likely to win the next election. `None` for a group whose only voter is
/// this node.
fn step_down_target(metrics: &RaftMetrics<C>) -> Option<u64> {
    let matched = |voter: u64| {
        metrics
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&voter))
            .and_then(|matched| matched.as_ref())
            .map(|log_id| log_id.index())
    };
    metrics
        .membership_config
        .voter_ids()
        .filter(|voter| *voter != metrics.id)
        .max_by_key(|voter| (matched(*voter), std::cmp::Reverse(*voter)))
}

/// How a leader's step-down and campaign ([`step_down_then_elect`])
/// ended.
#[derive(Debug)]
pub enum StepDown {
    /// The leader granted itself the step-down vote, which dropped its
    /// leader state, and then started a campaign (with a pre-vote round,
    /// that round).
    Campaigned,
    /// The node already holds a vote above the step-down vote: the
    /// leadership moved on without this step-down.
    Refused { by: UrsulaVote },
    /// The leader refused the step-down vote by its own vote: it appended
    /// an entry after the step-down read its last log id
    /// ([`StepDownVote::LogAdvanced`]).
    LogAdvanced,
}

/// How a leader's [`step_down`] vote went.
#[derive(Debug)]
pub(crate) enum StepDownVote {
    /// The leader granted itself the step-down vote, which dropped its
    /// leader state.
    Granted,
    /// The node already holds a vote above the step-down vote: the
    /// leadership moved on without this step-down.
    Refused { by: UrsulaVote },
    /// The node still holds the vote the step-down was made from and
    /// refused it for its log: the leader appended an entry after the
    /// step-down read its last log id, and OpenRaft refuses a vote whose
    /// last log id is below its own. It still leads.
    LogAdvanced,
}

/// Why a leader's step-down ([`step_down`], [`step_down_then_elect`]) did
/// not finish.
#[derive(Debug, thiserror::Error)]
pub enum StepDownError {
    #[error("submit the step-down vote")]
    Vote(#[source] RaftError<C>),
    #[error("start the campaign after a step-down")]
    Campaign(#[source] Fatal<C>),
    /// The step-down named this node: a vote for itself is its own
    /// candidacy, under which OpenRaft keeps the leader state.
    #[error("a leader steps down to another node, not to itself (node {target})")]
    TargetIsSelf { target: u64 },
}

/// Steps the leader at `vote` down to `target` without campaigning: it
/// grants itself [`transfer_step_down`], which drops the leader state as for
/// any follower. It sends no heartbeat from then on, so the other voters
/// elect a leader once their election timeout passes. `target` must be
/// another node ([`StepDownError::TargetIsSelf`]).
pub(crate) async fn step_down<SM: RaftStateMachine<C>>(
    raft: &Raft<C, SM>,
    vote: UrsulaVote,
    target: u64,
) -> Result<StepDownVote, StepDownError> {
    if raft.metrics().borrow_watched().id == target {
        return Err(StepDownError::TargetIsSelf { target });
    }
    // Read in the Raft core, so only the appends queued between this read
    // and the vote can move the last log id past it.
    let last_log_id = raft
        .with_raft_state(|state| state.log_ids.last().cloned())
        .await
        .map_err(|stopped| StepDownError::Vote(RaftError::Fatal(stopped)))?;
    let stepped_down = raft
        .vote(transfer_step_down(vote, target, last_log_id))
        .await
        .map_err(StepDownError::Vote)?;
    if stepped_down.vote_granted {
        Ok(StepDownVote::Granted)
    } else if stepped_down.vote == vote {
        Ok(StepDownVote::LogAdvanced)
    } else {
        Ok(StepDownVote::Refused {
            by: stepped_down.vote,
        })
    }
}

/// How a leadership transfer this leader submitted ended, as it sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferEnd {
    /// The leadership or the vote changed: the transfer moved it, or
    /// something else did.
    Moved,
    /// The target answered that it does not take the transfer.
    Refused,
    /// The leadership did not move in time.
    Stalled,
}

/// How long a leader waits on a leadership transfer it submitted
/// ([`transfer_end`]).
#[derive(Debug, Clone, Copy)]
struct TransferBounds {
    /// The longest wait: the transfer deadline.
    deadline: Duration,
    /// The wait after the target answered that it takes the transfer: one
    /// minimum election timeout, since it campaigns at once.
    retry: Duration,
    /// The wait, from the transfer's start, after the target answered that
    /// it holds another vote or its answer was lost: one leader lease, which
    /// a newer election or the target's own campaign needs at most.
    lease: Duration,
}

/// Waits until the transfer to `target` submitted at `vote` ends
/// ([`TransferEnd`]), as `bounds` says. A target's refusal ends it at once.
/// A leader without the target's answers waits for the deadline.
async fn transfer_end<SM: RaftStateMachine<C>>(
    raft: &Raft<C, SM>,
    vote: UrsulaVote,
    target: u64,
    bounds: TransferBounds,
    mut answers: watch::Receiver<Option<TransferAnswer>>,
) -> TransferEnd {
    let start = crate::rt::time::Instant::now();
    let mut bound = bounds.deadline;
    loop {
        let wait = bound.saturating_sub(start.elapsed());
        crate::rt::select! {
            biased;
            moved = settles_within(raft, vote, wait) => {
                return if moved { TransferEnd::Moved } else { TransferEnd::Stalled };
            }
            outcome = target_answer(&mut answers, vote, target) => match outcome {
                Some(TransferOutcome::Refused) => return TransferEnd::Refused,
                Some(TransferOutcome::Taken) => {
                    bound = bound.min(start.elapsed().saturating_add(bounds.retry));
                }
                Some(TransferOutcome::Superseded | TransferOutcome::Unanswered) => {
                    bound = bound.min(bounds.lease);
                }
                None => {
                    let moved = settles_within(raft, vote, wait).await;
                    return if moved { TransferEnd::Moved } else { TransferEnd::Stalled };
                }
            },
        }
    }
}

/// The next answer of `target` to the transfer this leader sent at `vote`.
/// `None` once no client of the engine can answer any more.
async fn target_answer(
    answers: &mut watch::Receiver<Option<TransferAnswer>>,
    vote: UrsulaVote,
    target: u64,
) -> Option<TransferOutcome> {
    loop {
        answers.changed().await.ok()?;
        let answer = *answers.borrow_and_update();
        if let Some(answer) = answer
            && answer.from == vote
            && answer.to == target
        {
            return Some(answer.outcome);
        }
    }
}

/// Repeats the pre-vote round of the leader at `vote` that stepped down
/// from its transfer to `target` ([`OwnerRaftHandle::abandon_stalled_transfer`])
/// every `retry` until a leader is known, for at most `budget`. A pre-vote
/// round raises no term, so a repeated one cannot unseat a leader a quorum
/// follows. The budget, one transfer deadline, outlasts the lease of the
/// old leader every voter holds (one maximum election timeout), so a quorum
/// that can reach this node grants a round within it. Past it, the voters'
/// election ticks elect.
async fn lead_again<SM: RaftStateMachine<C>>(
    raft: &Raft<C, SM>,
    vote: UrsulaVote,
    target: u64,
    retry: Duration,
    budget: Duration,
) {
    let start = crate::rt::time::Instant::now();
    loop {
        // A leader under the transfer's vote is this node's stale view of
        // itself before the step-down: the step-down raised the vote.
        let known = raft
            .wait(Some(retry))
            .metrics(
                |metrics| metrics.current_leader.is_some() && metrics.vote != vote,
                "a leader is known after an abandoned leadership transfer",
            )
            .await;
        if !matches!(known, Err(openraft::metrics::WaitError::Timeout(..))) {
            return;
        }
        if start.elapsed() >= budget {
            tracing::warn!(
                node_id = raft.metrics().borrow_watched().id,
                target,
                %vote,
                ?budget,
                "no leader after an abandoned leadership transfer; leaving the election to the \
                 election timers"
            );
            return;
        }
        if let Err(error) = raft.trigger().elect(true).await {
            tracing::warn!(
                %error,
                target,
                %vote,
                "the campaign after an abandoned leadership transfer did not start"
            );
            return;
        }
    }
}

/// Whether the replica's vote changes from `vote`, or it stops leading,
/// within `wait`.
async fn settles_within<SM: RaftStateMachine<C>>(
    raft: &Raft<C, SM>,
    vote: UrsulaVote,
    wait: Duration,
) -> bool {
    let settled = raft
        .wait(Some(wait))
        .metrics(
            |metrics| metrics.vote != vote || metrics.state != openraft::ServerState::Leader,
            "leadership transfer settles",
        )
        .await;
    !matches!(settled, Err(openraft::metrics::WaitError::Timeout(..)))
}

/// Steps the leader at `vote` down to `target`, then campaigns at once in
/// the next term ([`step_down_then_elect`] with [`Candidacy::Immediate`]):
/// a fault that ends a leadership without moving it.
#[cfg(any(test, madsim, feature = "fault-injection"))]
pub(crate) async fn step_down_and_campaign<SM: RaftStateMachine<C>>(
    raft: &Raft<C, SM>,
    vote: UrsulaVote,
    target: u64,
) -> Result<StepDown, StepDownError> {
    step_down_then_elect(raft, vote, target, Candidacy::Immediate).await
}

/// How a leader that stepped down campaigns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Candidacy {
    /// A pre-vote round first: the term moves only once a quorum would
    /// grant the vote, so a leader a quorum already follows keeps leading.
    PreVote,
    /// In the next term at once, even against a leader its voters follow.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    Immediate,
}

/// Steps the leader at `vote` down to `target`, then campaigns as
/// `candidacy` says.
///
/// OpenRaft ignores `elect` on a leader (#721). So the leader first steps
/// down ([`step_down`]) and campaigns only then.
async fn step_down_then_elect<SM: RaftStateMachine<C>>(
    raft: &Raft<C, SM>,
    vote: UrsulaVote,
    target: u64,
    candidacy: Candidacy,
) -> Result<StepDown, StepDownError> {
    match step_down(raft, vote, target).await? {
        StepDownVote::Granted => {}
        StepDownVote::Refused { by } => return Ok(StepDown::Refused { by }),
        StepDownVote::LogAdvanced => return Ok(StepDown::LogAdvanced),
    }
    raft.trigger()
        .elect(candidacy == Candidacy::PreVote)
        .await
        .map_err(StepDownError::Campaign)?;
    Ok(StepDown::Campaigned)
}

/// Administrative triggers execute on the owner, including their task creation.
pub struct OwnerTrigger<
    SM: RaftStateMachine<C, SnapshotData = SnapshotData> = RaftGroupStateMachine,
>(OwnerRaftHandle<SM>);

macro_rules! trigger {
    ($name:ident ( $($arg:ident : $ty:ty),* )) => {
        pub async fn $name(&self, $($arg: $ty),*) -> Result<(), Fatal<C>> {
            self.0.call(move |raft| async move { raft.trigger().$name($($arg),*).await }).await?
        }
    };
}
impl<SM: RaftStateMachine<C, SnapshotData = SnapshotData>> OwnerTrigger<SM> {
    trigger!(snapshot());
    trigger!(elect(pre_vote: bool));
    trigger!(heartbeat());
    trigger!(purge_log(upto: u64));
    trigger!(transfer_leader(to: u64));
}

/// Keep the transport classification structured while recording the original fatal cause.
pub(crate) fn owner_stopped(error: impl std::fmt::Display) -> GroupEngineError {
    tracing::warn!(%error, "owner Raft operation stopped");
    GroupEngineError::Infra(ursula_runtime::GroupInfraError::OwnerStopped)
}

pub(crate) fn owner_raft_error(error: RaftError<C>) -> GroupEngineError {
    match error {
        RaftError::Fatal(error) => owner_stopped(error),
        RaftError::APIError(impossible) => match impossible {},
    }
}
