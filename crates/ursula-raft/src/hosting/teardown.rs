//! The teardown executor and the owned-object passes of the hosting driver
//! (design 7.1 to 7.3).
//!
//! The driver runs the kernel's [`Teardown`] for each replica its plan
//! tears down, from the confirmed plan's `TearDown` step, at most
//! [`TEARDOWNS_PER_NODE`] at once and none while the node fence refuses.
//! Each step is one driver action of its group:
//!
//! - 0, ask: the membership asker asks at once and on its own cadence, and
//!   only an answer it got after the teardown started counts. Anything but
//!   a live leader's `NotMember` keeps the teardown asking, and the plan
//!   cancels it here once the view gives the node a role again.
//! - 1, fence: the plan admits no replica past step 0, so the pass's
//!   hosting set drops the group and no request reopens it.
//! - 2 and 3, unregister and stop: [`GroupHost::close`].
//! - 4, work ends: the group's engine work ledger, so nothing the stopped
//!   engine runs writes for the group again.
//! - 5, ask again and retire: a fresh ask along the group's leader
//!   candidates. A live leader's `NotMember` retires the log
//!   ([`RaftWal::retire_group`]), anything else, no answer included,
//!   admits the replica again with its log intact. A retirement the writer
//!   failed stays in doubt until a restart, so the step is retried and
//!   every open of the group meanwhile refuses: that is the retirement
//!   holding, not an open that is stuck.
//! - 6, forget: [`GroupHost::forget`], the snapshot coordinators' entries of
//!   the group, whose log then leaves the node's hard limit, and its left
//!   mark.
//!
//! The plan then reports the retirement (`ReportTornDown`), which the
//! driver submits as one more action of the group.
//!
//! **Owned objects.** A group's snapshot objects under the node's directory
//! go by its live incarnation's GC. Every other one belongs to no replica,
//! so [`ObjectPasses`] deletes it: at the driver's start, again one
//! `raft.install_snapshot_timeout` later, since a PUT of the previous
//! process may still land, and for a retired group right after its
//! retirement and again that long later. A start's pass lists the groups
//! the node's directory holds ([`SnapshotStore::list_node_groups`]). Each
//! group's pass lists its objects and deletes all of them, and runs only
//! while the hosting set does not admit the group, its log is absent or
//! retired on a core that opened and no engine of it is registered. It is
//! one action of the group, so an open waits for it, and the hosting set
//! withholds a `Create` of the group until it ended. A pass the plan
//! admits first is dropped: the new replica's restore owes its own. At
//! most [`OBJECT_PASSES_PER_NODE`] run at once, none while the node fence
//! refuses, and the fenced store refuses their deletes anyway.
//!
//! [`SnapshotStore::list_node_groups`]: ursula_runtime::SnapshotStore::list_node_groups

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use ursula_control::Admission;
use ursula_control::CandidatesOf;
use ursula_control::LocalLog;
use ursula_control::LocalReplica;
use ursula_control::PlacementView;
use ursula_control::Teardown;
use ursula_control::TeardownEnd;
use ursula_control::TeardownEvent;
use ursula_control::TeardownOutcome;
use ursula_control::TeardownStep;
use ursula_control::leader_candidates;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::TeardownStage;
use ursula_runtime::SnapshotStoreError;
use ursula_shard::RaftGroupId;

use super::ActionEnd;
use super::Actions;
use super::GroupHost;
use super::HostingDriver;
use super::PLAN_INTERVAL;
use super::asker;
use super::asker::Askers;
use super::asker::GroupAnswer;
use crate::RetireGroupError;
use crate::log_store::CoreJournalError;
use crate::rt::sync::oneshot;
use crate::rt::task::JoinHandle;
use crate::rt::time::Instant;
use crate::snapshot_holds::SNAPSHOT_GC_RETRY;

/// The most teardowns a node runs at once, at any step (design 7.3).
pub const TEARDOWNS_PER_NODE: usize = 8;

/// The most owned-object passes a node runs at once (design 7.3).
pub const OBJECT_PASSES_PER_NODE: usize = 8;

/// How a teardown step's action ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StepEnd {
    /// The step's work completed.
    Done,
    /// The second ask ended, with a live leader's answer or none.
    Answered(Option<GroupAnswer>),
}

/// One running teardown.
#[derive(Debug, Clone, Copy)]
struct Running {
    teardown: Teardown,
    /// The asker's answer count when step 0 last looked: only a later
    /// answer counts for it.
    answers_from: u64,
}

/// The node's running teardowns, by group.
#[derive(Debug, Default)]
pub(super) struct Teardowns(BTreeMap<RaftGroupId, Running>);

impl Teardowns {
    /// The teardown of `group`, if one runs.
    pub(super) fn state(&self, group: RaftGroupId) -> Option<Teardown> {
        self.0.get(&group).map(|running| running.teardown)
    }

    /// Whether the teardown of `group` passed step 0.
    pub(super) fn fenced(&self, group: RaftGroupId) -> bool {
        matches!(self.state(group), Some(Teardown::Running(_)))
    }

    /// The stage of every running teardown, for readiness.
    pub(super) fn stages(&self) -> BTreeMap<RaftGroupId, TeardownStage> {
        self.0
            .iter()
            .map(|(group, running)| (*group, running.teardown.stage()))
            .collect()
    }

    /// Starts tearing `group` down at step 0, unless one runs or the node
    /// runs [`TEARDOWNS_PER_NODE`]. Whether it started.
    pub(super) fn start(&mut self, group: RaftGroupId, askers: &Askers) -> bool {
        if self.0.contains_key(&group) || self.0.len() >= TEARDOWNS_PER_NODE {
            return false;
        }
        self.0.insert(group, Running {
            teardown: Teardown::start(),
            answers_from: askers.answer_count(group),
        });
        true
    }

    /// The teardown of `group` after `event`, forgetting it once it ended.
    pub(super) fn advance(
        &mut self,
        group: RaftGroupId,
        event: TeardownEvent,
    ) -> Option<TeardownOutcome> {
        let running = self.0.get_mut(&group)?;
        let outcome = running.teardown.advance(event);
        match outcome {
            TeardownOutcome::Continue(next) => running.teardown = next,
            TeardownOutcome::Ended(end) => {
                self.0.remove(&group);
                match end {
                    TeardownEnd::Retired => tracing::info!(
                        group = group.0,
                        "hosting: the replica is torn down and its log retired"
                    ),
                    TeardownEnd::Readmitted => tracing::info!(
                        group = group.0,
                        "hosting: the group did not answer again that this node is no member; \
                         the replica is admitted again with its log"
                    ),
                    TeardownEnd::Cancelled => tracing::debug!(
                        group = group.0,
                        "hosting: the teardown is cancelled before anything was fenced"
                    ),
                }
            }
        }
        Some(outcome)
    }

    /// Step 0 of `group`'s teardown: the asker's latest answer, if it came
    /// after the step last looked.
    fn step_zero(&mut self, group: RaftGroupId, askers: &Askers) {
        let Some(running) = self.0.get_mut(&group) else {
            return;
        };
        let Some(answer) = askers.answer_since(group, running.answers_from) else {
            return;
        };
        running.answers_from = askers.answer_count(group);
        self.advance(group, TeardownEvent::Answered(Some(answer.answer)));
    }

    /// Records how the action of `group`'s step `at` ended. An end of a
    /// step the teardown is no longer at changes nothing. One close runs
    /// steps 2 and 3. Whether the step retired the log.
    pub(super) fn step_ended(
        &mut self,
        group: RaftGroupId,
        at: TeardownStep,
        end: StepEnd,
    ) -> bool {
        if self.state(group) != Some(Teardown::Running(at)) {
            return false;
        }
        let event = match end {
            StepEnd::Done => TeardownEvent::StepDone,
            StepEnd::Answered(answer) => {
                TeardownEvent::Answered(answer.map(|answer| answer.answer))
            }
        };
        let outcome = self.advance(group, event);
        if at == TeardownStep::Unregister
            && outcome
                == Some(TeardownOutcome::Continue(Teardown::Running(
                    TeardownStep::Stop,
                )))
        {
            self.advance(group, TeardownEvent::StepDone);
        }
        at == TeardownStep::Retire && end == StepEnd::Done
    }

    fn groups(&self) -> Vec<RaftGroupId> {
        self.0.keys().copied().collect()
    }
}

/// When a pass is owed: the earliest and the latest due time only, since a
/// pass covers every reason to run one before it.
#[derive(Debug, Clone, Copy)]
struct Owed {
    earliest: Instant,
    latest: Instant,
}

impl Owed {
    fn at(at: Instant) -> Self {
        Self {
            earliest: at,
            latest: at,
        }
    }

    fn owe(&mut self, at: Instant) {
        self.earliest = self.earliest.min(at);
        self.latest = self.latest.max(at);
    }

    /// What stays owed once a pass starts at `now`, `None` when nothing.
    fn after_pass(self, now: Instant) -> Option<Self> {
        (self.latest > now).then_some(Self::at(self.latest))
    }
}

/// A listing of the node's directory that runs.
struct Listing {
    task: JoinHandle<()>,
    answer: oneshot::Receiver<Result<BTreeSet<RaftGroupId>, SnapshotStoreError>>,
}

/// The node's owned-object passes (see the module docs).
pub(super) struct ObjectPasses {
    /// `raft.install_snapshot_timeout`: how long a failed PUT may take to
    /// land.
    late: std::time::Duration,
    listings: Option<Owed>,
    listing: Option<Listing>,
    owed: BTreeMap<RaftGroupId, Owed>,
    running: BTreeSet<RaftGroupId>,
}

impl ObjectPasses {
    /// The passes of a driver that starts now: a listing now and one
    /// `late` later.
    pub(super) fn starting(late: std::time::Duration) -> Self {
        let now = Instant::now();
        let mut listings = Owed::at(now);
        if let Some(later) = now.checked_add(late) {
            listings.owe(later);
        }
        Self {
            late,
            listings: Some(listings),
            listing: None,
            owed: BTreeMap::new(),
            running: BTreeSet::new(),
        }
    }

    /// Whether a pass of `group` runs.
    pub(super) fn running(&self, group: RaftGroupId) -> bool {
        self.running.contains(&group)
    }

    /// Owes a pass of `group`, whose log was just retired: now, and one
    /// install timeout later.
    pub(super) fn retired(&mut self, group: RaftGroupId) {
        let now = Instant::now();
        self.owe(group, now);
        if let Some(later) = now.checked_add(self.late) {
            self.owe(group, later);
        }
    }

    /// Records how the pass of `group` ended. A failed one runs again
    /// [`SNAPSHOT_GC_RETRY`] later.
    pub(super) fn ended(&mut self, group: RaftGroupId, collected: bool) {
        self.running.remove(&group);
        if !collected && let Some(at) = Instant::now().checked_add(SNAPSHOT_GC_RETRY) {
            self.owe(group, at);
        }
    }

    fn owe(&mut self, group: RaftGroupId, at: Instant) {
        self.owed
            .entry(group)
            .and_modify(|owed| owed.owe(at))
            .or_insert(Owed::at(at));
    }

    /// Takes the answer of a listing that ended, and owes a pass of every
    /// group it names now. A failed listing runs again
    /// [`SNAPSHOT_GC_RETRY`] later.
    fn reap_listing(&mut self) {
        let Some(listing) = &mut self.listing else {
            return;
        };
        let answer = match listing.answer.try_recv() {
            Ok(answer) => answer.map_err(Some),
            Err(oneshot::error::TryRecvError::Empty) => return,
            Err(oneshot::error::TryRecvError::Closed) => Err(None),
        };
        self.listing = None;
        let now = Instant::now();
        match answer {
            Ok(groups) => {
                for group in groups {
                    self.owe(group, now);
                }
            }
            Err(error) => {
                match error {
                    Some(error) => tracing::warn!(
                        %error,
                        "hosting: the node's snapshot directory did not list; listing again later"
                    ),
                    None => tracing::warn!(
                        "hosting: the listing of the node's snapshot directory ended without an answer; listing again later"
                    ),
                }
                if let Some(at) = now.checked_add(SNAPSHOT_GC_RETRY) {
                    self.listings.get_or_insert(Owed::at(at)).owe(at);
                }
            }
        }
    }
}

impl Drop for ObjectPasses {
    /// A stopped driver stops its listing.
    fn drop(&mut self) {
        if let Some(listing) = &self.listing {
            listing.task.abort();
        }
    }
}

impl<H: GroupHost> HostingDriver<H> {
    /// Runs the next step of every teardown whose group has no action
    /// running: step 0 reads the asker's answers, step 1 passes once the
    /// published hosting set dropped the group, and every later step is
    /// one action. Nothing runs while the node fence refuses.
    pub(super) fn tear_down(
        &self,
        actions: &mut Actions,
        teardowns: &mut Teardowns,
        askers: &Askers,
        published: &BTreeMap<RaftGroupId, Admission>,
        view: &PlacementView,
        peers: &BTreeSet<NodeId>,
    ) {
        if !self.wal.fence().admits_now() {
            return;
        }
        for group in teardowns.groups() {
            if teardowns.state(group) == Some(Teardown::Asking) {
                teardowns.step_zero(group, askers);
            }
            if teardowns.state(group) == Some(Teardown::Running(TeardownStep::Fence))
                && !published.contains_key(&group)
            {
                teardowns.advance(group, TeardownEvent::StepDone);
            }
            let Some(Teardown::Running(at)) = teardowns.state(group) else {
                continue;
            };
            let done = move |end| ActionEnd::Teardown { at, end };
            match at {
                TeardownStep::Fence => {}
                TeardownStep::Unregister | TeardownStep::Stop => {
                    let host = Arc::clone(&self.host);
                    self.begin(actions, group, async move {
                        match host.close(group).await {
                            Ok(()) => done(StepEnd::Done),
                            Err(error) => {
                                tracing::warn!(
                                    group = group.0,
                                    %error,
                                    "hosting: the replica being torn down did not close; retrying"
                                );
                                ActionEnd::Failed
                            }
                        }
                    });
                }
                TeardownStep::AwaitIdle => {
                    let wal = self.wal.clone();
                    self.begin(actions, group, async move {
                        let ended = wal.engine_work_ended(group);
                        match crate::rt::time::timeout(PLAN_INTERVAL, ended).await {
                            Ok(()) => done(StepEnd::Done),
                            Err(_elapsed) => {
                                tracing::debug!(
                                    group = group.0,
                                    "hosting: the stopped engine's work still runs"
                                );
                                ActionEnd::Failed
                            }
                        }
                    });
                }
                TeardownStep::AskAgain => {
                    let candidates: Vec<NodeId> =
                        leader_candidates(self.me, CandidatesOf::Group(group), view, None, peers)
                            .iter()
                            .collect();
                    let asked = asker::ask(
                        self.meta.clone(),
                        self.registry.left_marks().clone(),
                        self.me,
                        group,
                        candidates,
                    );
                    self.begin(actions, group, async move {
                        done(StepEnd::Answered(asked.await))
                    });
                }
                TeardownStep::Retire => {
                    let wal = self.wal.clone();
                    self.begin(actions, group, async move {
                        match wal.retire_group(group).await {
                            Ok(()) => done(StepEnd::Done),
                            // The retirement is in doubt until a restart.
                            Err(
                                error @ RetireGroupError::Journal {
                                    source: CoreJournalError::Retiring { .. },
                                    ..
                                },
                            ) => {
                                tracing::debug!(group = group.0, %error, "hosting: the log's retirement is in doubt");
                                ActionEnd::Failed
                            }
                            Err(error) => {
                                tracing::warn!(
                                    group = group.0,
                                    %error,
                                    "hosting: the torn-down replica's log was not retired; retrying"
                                );
                                ActionEnd::Failed
                            }
                        }
                    });
                }
                TeardownStep::ForgetMetrics => {
                    let host = Arc::clone(&self.host);
                    let registry = self.registry.clone();
                    self.begin(actions, group, async move {
                        host.forget(group).await;
                        registry.forget_group_snapshots(group);
                        registry.left_marks().forget(group);
                        done(StepEnd::Done)
                    });
                }
            }
        }
    }

    /// Reports each retirement the plan reports torn down, one action of
    /// its group. A refused or lost report is sent again by a later pass,
    /// and the meta group takes a repeated one as nothing to tear down.
    pub(super) fn report_torn_down(
        &self,
        actions: &mut Actions,
        reports: Vec<(RaftGroupId, MetaLogId)>,
    ) {
        if !self.wal.fence().admits_now() {
            return;
        }
        for (group, created_at) in reports {
            let meta = self.meta.clone();
            self.begin(actions, group, async move {
                match meta.report_torn_down(group, created_at).await {
                    Ok(_applied) => ActionEnd::Done,
                    Err(error) => {
                        tracing::debug!(
                            group = group.0,
                            %error,
                            "hosting: the teardown report did not apply; reporting again"
                        );
                        ActionEnd::Failed
                    }
                }
            });
        }
    }

    /// Starts the owned-object passes due now (see the module docs).
    pub(super) fn collect_objects(
        &self,
        actions: &mut Actions,
        passes: &mut ObjectPasses,
        teardowns: &Teardowns,
        local: &BTreeMap<RaftGroupId, LocalReplica>,
        published: &BTreeMap<RaftGroupId, Admission>,
    ) {
        if !self.wal.fence().admits_now() {
            return;
        }
        passes.reap_listing();
        let now = Instant::now();
        if passes.listing.is_none()
            && let Some(listings) = passes.listings
            && listings.earliest <= now
        {
            passes.listings = listings.after_pass(now);
            let (answer_tx, answer) = oneshot::channel();
            let store = self.registry.snapshot_store();
            let me = self.me.get();
            let task = crate::rt::spawn(async move {
                // The driver stopped: nothing waits for the answer.
                let _closed = answer_tx.send(store.list_node_groups(me).await);
            });
            passes.listing = Some(Listing { task, answer });
        }
        let registered = self.registry.registered_groups();
        let due: Vec<(RaftGroupId, Owed)> = passes
            .owed
            .iter()
            .filter(|(_, owed)| owed.earliest <= now)
            .map(|(group, owed)| (*group, *owed))
            .collect();
        for (group, owed) in due {
            if passes.running.len() >= OBJECT_PASSES_PER_NODE {
                break;
            }
            let log = local.get(&group).map(|replica| replica.log);
            if published.contains_key(&group)
                || matches!(log, Some(LocalLog::Empty | LocalLog::Holding))
            {
                // A replica of the group may run: its incarnation owns the
                // objects, and its restore owes its own pass.
                passes.owed.remove(&group);
                continue;
            }
            // A teardown or an engine of the group still runs, or its core
            // has not opened: the pass waits.
            if teardowns.state(group).is_some()
                || registered.contains(&group)
                || log == Some(LocalLog::Unknown)
            {
                continue;
            }
            let store = self.registry.snapshot_store();
            let began = self.begin(actions, group, async move {
                match crate::snapshot_holds::collect_unowned(&store, group).await {
                    Ok(_deleted) => ActionEnd::Objects { collected: true },
                    Err(error) => {
                        tracing::warn!(
                            group = group.0,
                            %error,
                            "hosting: the objects of a group no replica here runs were not collected; retrying later"
                        );
                        ActionEnd::Objects { collected: false }
                    }
                }
            });
            if began {
                passes.running.insert(group);
                match owed.after_pass(now) {
                    Some(rest) => passes.owed.insert(group, rest),
                    None => passes.owed.remove(&group),
                };
            }
        }
    }
}
