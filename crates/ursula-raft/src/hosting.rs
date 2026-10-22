//! What this node runs: the hosting driver (design 4.1 to 4.3).
//!
//! One owner decides which data replicas a node runs, the hosting driver
//! ([`HostingDriver`]), around the kernel's pure [`plan_hosting`]. It alone
//! writes the node's [`HostingSet`], which the engine factory reads as the
//! admission of every open ([`crate::MetaPlacement`]). A core opens a group
//! lazily on its first request, so the set is the only way a replica is
//! created, and a confirmed placement is the only way into the set with
//! `Create` (4.2).
//!
//! A pass reads what the journals hold ([`RaftWal::local_groups`]) first,
//! then confirms the view when the plan asks for it, then plans. So a new
//! confirming barrier comes after the inputs the plan reads. The driver
//! plans on every view change, every change of the registry's engines,
//! every registration answer, every end of one of its opens and closes and
//! every [`PLAN_INTERVAL`], and confirms at most once per interval. Until
//! a confirmation is one interval old, it also confirms every later view
//! that still carries its stamp ([`ConfirmedView::new`]), with no new
//! barrier. So the `Create` admissions of a confirmed plan hold between
//! two confirmations, and a plan may read a confirmation up to one
//! interval older than its inputs. Each pass then:
//!
//! - publishes the plan's admissions in the hosting set. For every replica
//!   the set drops this is the first step of its close: no request reopens
//!   it.
//! - opens every admitted group the node does not run, `Create` and `Open`
//!   alike, with at most [`ACTIONS_PER_CORE`] opens and closes running per
//!   core. No failed open stops anything: the group stays closed under its
//!   open back-off and a pass one [`PLAN_INTERVAL`] later opens it again.
//!   A failed close is retried likewise.
//! - closes every running replica the plan does not admit (7.3's steps 1
//!   to 3), then forgets its gauges and its open back-off.
//! - asks, for each held replica that may have left its group, the
//!   group's leader whether the node is still a member (`hosting::asker`),
//!   with the room the opens and closes leave in each core's budget.
//! - publishes a [`HostingStatus`] for readiness, with the latest answer
//!   of each group.
//! - refreshes the elections of every running replica whose placed voters
//!   made it its group's only voter and no longer do, or the reverse
//!   ([`RaftGroupHandleRegistry::refresh_only_voter_elections`]): the shed
//!   exemption of an only voter reads those voters, and the election switch
//!   holds what it read last.
//!
//! A node that knows itself removed, by a `NodeRemoved` answer or by its
//! view ([`ursula_control::Standing::knows_removed`]), marks its `PlacementUnavailable`
//! leadership shed for the rest of the run, and its plan closes each
//! replica once it stops counting (design 8.1).
//!
//! While the node fence refuses, the driver creates and closes nothing:
//! every admission it publishes is `Open`, and no group leaves the set.
//!
//! The teardown executor comes later (S4). The driver logs a plan's
//! teardown and report steps and starts neither, so such a replica stays
//! admitted, which is the safe direction. Each held replica counts as a
//! voter by the one voter predicate
//! ([`RaftGroupHandleRegistry::counts_as_voter`]).
//!
//! A [`GroupHost`] runs the replicas: the server's over its shard runtime,
//! the simulation's over its cluster.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::rt::WatchReceiver;
use ursula_control::Admission;
use ursula_control::CandidatesOf;
use ursula_control::ConfirmedStamp;
use ursula_control::ConfirmedView;
use ursula_control::HostingInput;
use ursula_control::HostingPlan;
use ursula_control::HostingStep;
use ursula_control::LocalReplica;
use ursula_control::PlacementView;
use ursula_control::ViewInput;
use ursula_control::leader_candidates;
use ursula_control::plan_hosting;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ReplicaRole;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;

use crate::LeadershipShedReason;
use crate::RaftGroupHandleRegistry;
use crate::RaftWal;
use crate::meta::status::MetaStatusReader;
use crate::rt::sync::mpsc;
use crate::rt::sync::watch;
use crate::rt::task::JoinHandle;
use crate::rt::time::Instant;

mod asker;

pub use asker::ASK_INTERVAL;
use asker::Askers;
pub use asker::GroupAnswer;

/// How often the driver plans without a trigger, and the least time
/// between two of its confirmations.
pub const PLAN_INTERVAL: Duration = Duration::from_secs(1);

/// The most opens and closes the driver runs at once on one core (#746).
pub const ACTIONS_PER_CORE: usize = 8;

/// What runs a node's data replicas for the hosting driver.
pub trait GroupHost: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Opens the replica of `group`, as its first request would, with the
    /// admission the hosting set holds, and returns once the open ended.
    fn open(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), Self::Error>>;

    /// Closes the replica of `group`: unregisters its handle
    /// ([`RaftGroupHandleRegistry::unregister_group`]), then stops its
    /// engine and waits until the engine released its store (design 7.3,
    /// steps 2 and 3).
    fn close(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), Self::Error>>;

    /// Forgets what the node kept of a closed replica of `group`: its
    /// gauges and its open back-off (7.3, step 6).
    fn forget(&self, group: RaftGroupId) -> BoxFuture<'_, ()>;
}

/// The data groups this node may run, and how each may open. Only the
/// hosting driver writes it. Cloning shares it.
#[derive(Debug, Clone, Default)]
pub struct HostingSet(Arc<arc_swap::ArcSwap<BTreeMap<RaftGroupId, Admission>>>);

impl HostingSet {
    /// How `group` may open, `None` when the node does not host it.
    pub fn admission(&self, group: RaftGroupId) -> Option<Admission> {
        self.0.load().get(&group).copied()
    }

    /// Every admitted group.
    pub fn admitted(&self) -> Arc<BTreeMap<RaftGroupId, Admission>> {
        self.0.load_full()
    }

    fn publish(&self, admitted: BTreeMap<RaftGroupId, Admission>) {
        self.0.store(Arc::new(admitted));
    }
}

/// What the hosting driver did last, for readiness. Before its first pass
/// it is the default: nothing held, admitted or confirmed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostingStatus {
    /// The groups the journals held at the last pass
    /// ([`LocalLog::is_held`](ursula_control::LocalLog::is_held)).
    pub held: BTreeSet<RaftGroupId>,
    /// The hosting set the last pass published.
    pub admitted: BTreeMap<RaftGroupId, Admission>,
    /// The last plan's held replicas without a role in the bootstrapped
    /// view, or, on a removed node, the replicas it keeps open.
    pub unplaced: BTreeSet<RaftGroupId>,
    /// The stamp of the driver's latest confirmation in this run, `None`
    /// before the first. The last plan may be older or newer than it:
    /// `admitted` comes from an unconfirmed view whenever the plan needed
    /// no confirmation or its confirmation failed, and then holds only
    /// `Open` admissions of what the journals held. So the stamp says only
    /// that a view was confirmed in this run, never that `admitted` was
    /// planned from it.
    pub last_confirmed: Option<ConfirmedStamp>,
    /// The latest live leader's answer about each held replica the
    /// membership asker asked for (`hosting::asker`).
    pub answers: BTreeMap<RaftGroupId, GroupAnswer>,
}

/// A reader of the hosting driver's status.
#[derive(Debug, Clone)]
pub struct HostingStatusWatch(watch::Receiver<Arc<HostingStatus>>);

impl HostingStatusWatch {
    /// The status the last pass published.
    pub fn current(&self) -> Arc<HostingStatus> {
        Arc::clone(&self.0.borrow())
    }

    /// Waits until a pass publishes another status. `false` once the
    /// driver stopped.
    pub async fn changed(&mut self) -> bool {
        self.0.changed().await.is_ok()
    }
}

/// The running hosting driver. Its opens and closes stop with it.
#[derive(Debug)]
pub struct HostingTask {
    task: JoinHandle<()>,
    status: HostingStatusWatch,
}

impl HostingTask {
    /// A reader of the status each pass publishes.
    pub fn status(&self) -> HostingStatusWatch {
        self.status.clone()
    }

    /// Stops the driver and the opens and closes it runs.
    pub async fn shutdown(self) {
        self.task.abort();
        if let Err(error) = self.task.await
            && !error.is_cancelled()
        {
            tracing::error!(%error, "hosting: the driver failed");
        }
    }
}

/// This node's hosting driver (see the module docs).
pub struct HostingDriver<H> {
    me: NodeId,
    host: Arc<H>,
    wal: RaftWal,
    registry: RaftGroupHandleRegistry,
    meta: MetaStatusReader,
    hosting: HostingSet,
    status: watch::Sender<Arc<HostingStatus>>,
}

impl<H: GroupHost> HostingDriver<H> {
    /// Starts the driver of the node `meta` reports on, which runs its
    /// replicas on `host` and writes `hosting`. The caller starts it once
    /// meta published its first view, so the first plan opens what the
    /// journals hold (4.3).
    pub fn start(
        host: Arc<H>,
        wal: RaftWal,
        meta: MetaStatusReader,
        hosting: HostingSet,
    ) -> HostingTask {
        let (status, receiver) = watch::channel(Arc::default());
        let driver = Self {
            me: meta.node(),
            registry: meta.registry().clone(),
            host,
            wal,
            meta,
            hosting,
            status,
        };
        HostingTask {
            task: crate::rt::spawn(driver.run()),
            status: HostingStatusWatch(receiver),
        }
    }

    async fn run(self) {
        let mut views = self.registry.placement_watch();
        let mut groups = self.registry.subscribe_group_changes();
        let mut answers = self.meta.registration_answers();
        let (mut views_open, mut groups_open, mut answers_open) = (true, true, true);
        let mut actions = Actions::new();
        let mut confirms = Confirms::default();
        let mut askers = Askers::default();
        let mut only_voter = BTreeSet::new();
        loop {
            actions.reap(&mut askers);
            self.pass(&mut actions, &mut confirms, &mut askers).await;
            self.registry.refresh_only_voter_elections(&mut only_voter);
            let ended = crate::rt::select! {
                biased;
                changed = views.changed(), if views_open => {
                    views_open = changed;
                    None
                }
                changed = groups.changed(), if groups_open => {
                    groups_open = changed.is_ok();
                    None
                }
                changed = answers.changed(), if answers_open => {
                    answers_open = changed.is_ok();
                    None
                }
                ended = actions.done.recv() => ended,
                () = crate::rt::time::sleep(PLAN_INTERVAL) => None,
            };
            if let Some((group, end)) = ended {
                actions.end(group, end, &mut askers);
            }
        }
    }

    /// One pass: read the journals, confirm when the plan asks for it and
    /// the interval allows, plan, act on the plan, then ask about the held
    /// replicas that may have left their groups.
    async fn pass(&self, actions: &mut Actions, confirms: &mut Confirms, askers: &mut Askers) {
        let local = match self.wal.local_groups() {
            Ok(local) => local,
            Err(error) => {
                tracing::warn!(%error, "hosting: the journals list no groups; planning nothing");
                return;
            }
        };
        let local: BTreeMap<RaftGroupId, LocalReplica> = local
            .iter()
            .map(|(group, log)| {
                (group, LocalReplica {
                    log,
                    counts: self.registry.counts_as_voter(group),
                    teardown: None,
                })
            })
            .collect();
        let standing = self.meta.registration_answers().borrow().standing();
        let input = |view| HostingInput {
            me: self.me,
            standing,
            view,
            local: &local,
        };
        let view = self.registry.placement_watch().current();
        if standing.knows_removed(view.cluster(), self.me) {
            // A removed node takes no leadership for the rest of the run
            // (design 8.1): it closes what stopped counting, and a group it
            // led would never reconcile toward a placement that has no
            // role for it.
            self.registry
                .mark_leadership_shed(LeadershipShedReason::PlacementUnavailable);
        }
        let mut plan = plan_hosting(&input(ViewInput::Unconfirmed(&view)));
        if plan.steps.first() == Some(&HostingStep::ConfirmView) {
            if let Some(confirmed) = confirms.carried(&view) {
                plan = plan_hosting(&input(ViewInput::Confirmed(&confirmed)));
            } else if confirms.due() {
                let started = Instant::now();
                confirms.at = Some(started);
                match self.meta.confirm_view().await {
                    Ok(confirmed) => {
                        plan = plan_hosting(&input(ViewInput::Confirmed(&confirmed)));
                        confirms.last = Some(Confirmed {
                            at: started,
                            stamp: confirmed.stamp(),
                        });
                    }
                    Err(error) => tracing::debug!(
                        %error,
                        "hosting: the view is not confirmed; planning from the unconfirmed view"
                    ),
                }
            }
        }
        let confirmed = confirms.last.map(|last| last.stamp);
        let held: BTreeSet<RaftGroupId> = local
            .iter()
            .filter(|(_, replica)| replica.log.is_held())
            .map(|(group, _)| *group)
            .collect();
        askers.retain(|group| held.contains(&group));
        self.act(plan, actions, held, confirmed, askers.answers());
        self.ask(&local, &view, actions, askers);
    }

    /// Publishes the plan's admissions, opens what the node does not run
    /// and closes what it may no longer run.
    fn act(
        &self,
        plan: HostingPlan,
        actions: &mut Actions,
        held: BTreeSet<RaftGroupId>,
        confirmed: Option<ConfirmedStamp>,
        answers: BTreeMap<RaftGroupId, GroupAnswer>,
    ) {
        for step in &plan.steps {
            match *step {
                HostingStep::ConfirmView | HostingStep::CancelTeardown { .. } => {}
                HostingStep::TearDown { group } => tracing::debug!(
                    group = group.0,
                    "hosting: the plan tears this replica down; no teardown runs yet, so it stays admitted"
                ),
                HostingStep::ReportTornDown { group, .. } => tracing::debug!(
                    group = group.0,
                    "hosting: the plan reports this replica torn down; no report is sent yet"
                ),
            }
        }
        let fenced = !self.wal.fence().admits_now();
        let admitted = if fenced {
            // Nothing is created and nothing leaves the set.
            let previous = self.hosting.admitted();
            previous
                .keys()
                .chain(plan.admitted.keys())
                .map(|group| (*group, Admission::Open))
                .collect()
        } else {
            plan.admitted
        };
        // Step 1 of every close below: no request reopens a dropped group.
        self.hosting.publish(admitted.clone());
        let running = self.registry.registered_groups();
        if !fenced {
            for group in running.iter().copied() {
                if admitted.contains_key(&group) {
                    continue;
                }
                let host = Arc::clone(&self.host);
                self.begin(actions, group, async move {
                    match host.close(group).await {
                        Ok(()) => {
                            host.forget(group).await;
                            ActionEnd::Done
                        }
                        Err(error) => {
                            tracing::warn!(
                                group = group.0,
                                %error,
                                "hosting: the replica the plan does not admit did not close; retrying"
                            );
                            ActionEnd::Failed
                        }
                    }
                });
            }
        }
        for group in admitted.keys().copied() {
            if running.contains(&group) {
                continue;
            }
            let host = Arc::clone(&self.host);
            self.begin(actions, group, async move {
                match host.open(group).await {
                    Ok(()) => ActionEnd::Done,
                    Err(error) => {
                        tracing::debug!(
                            group = group.0,
                            %error,
                            "hosting: the admitted replica did not open; retrying"
                        );
                        ActionEnd::Failed
                    }
                }
            });
        }
        let status = HostingStatus {
            held,
            admitted,
            unplaced: plan.unplaced,
            last_confirmed: confirmed,
            answers,
        };
        self.status.send_if_modified(|current| {
            let modified = **current != status;
            if modified {
                *current = Arc::new(status);
            }
            modified
        });
    }

    /// Asks, for each held replica that may have left its group and is due
    /// ([`Askers::due`]), the group's leader candidates whether the node is
    /// still a member: a closed replica, and an open one that does not lead
    /// and whose bootstrapped view gives the node the role `Outgoing`,
    /// `Abandoned` or none. An open replica adds its effective membership to
    /// the candidates. While the node fence refuses, nothing is asked.
    fn ask(
        &self,
        local: &BTreeMap<RaftGroupId, LocalReplica>,
        view: &PlacementView,
        actions: &mut Actions,
        askers: &mut Askers,
    ) {
        if !self.wal.fence().admits_now() {
            return;
        }
        let peers: BTreeSet<NodeId> = self
            .registry
            .peer_cluster()
            .directory()
            .node_ids()
            .map(NodeId::new)
            .collect();
        let marks = self.registry.left_marks();
        for (group, replica) in local {
            if !replica.log.is_held() {
                continue;
            }
            let raft = self.registry.get(*group);
            let metrics = raft.as_ref().map(|raft| raft.metrics());
            let metrics = metrics.as_ref().map(|metrics| metrics.borrow_watched());
            if let Some(metrics) = &metrics {
                // Without a bootstrapped view an open replica counts by its
                // membership, and a leader knows its own: nothing says it
                // may have left.
                let placed = view.cluster().map(|cluster| cluster.role(*group, self.me));
                let may_have_left = matches!(
                    placed,
                    Some(None | Some(ReplicaRole::Outgoing | ReplicaRole::Abandoned))
                ) && metrics.current_leader != Some(self.me.get());
                if !may_have_left {
                    continue;
                }
            }
            let vote = metrics.as_ref().map(|metrics| metrics.vote);
            if !askers.due(*group, vote, marks) {
                continue;
            }
            let membership = metrics.as_ref().and_then(|metrics| {
                crate::memberships::membership_config(
                    &metrics.membership_config,
                    crate::MembershipKind::Effective,
                )
                .ok()
            });
            drop(metrics);
            let candidates: Vec<NodeId> = leader_candidates(
                self.me,
                CandidatesOf::Group(*group),
                view,
                membership.as_ref(),
                &peers,
            )
            .iter()
            .collect();
            let work = asker::ask(
                self.meta.clone(),
                marks.clone(),
                self.me,
                *group,
                candidates,
            );
            if self.begin(actions, *group, work) {
                askers.started(*group, vote, marks);
            }
        }
    }

    /// Starts `work` on `group` unless an action of the group runs, the
    /// group's last action failed less than [`PLAN_INTERVAL`] ago or its
    /// core runs [`ACTIONS_PER_CORE`] already. Whether it started.
    fn begin(
        &self,
        actions: &mut Actions,
        group: RaftGroupId,
        work: impl Future<Output = ActionEnd> + Send + 'static,
    ) -> bool {
        let Some(core) = self.wal.placement(group).map(|placement| placement.core_id) else {
            tracing::debug!(
                group = group.0,
                "hosting: the group is not in the shard map"
            );
            return false;
        };
        if actions.running.contains_key(&group)
            || actions.failed.contains_key(&group)
            || !actions.room(core)
        {
            return false;
        }
        let done = actions.done_tx.clone();
        let task = crate::rt::spawn(async move {
            let end = work.await;
            // The driver stopped: nothing waits for the end.
            let _closed = done.send((group, end));
        });
        actions.running.insert(group, Action { core, task });
        true
    }
}

/// When the driver last tried to confirm, and its last confirmation.
#[derive(Debug, Default)]
struct Confirms {
    at: Option<Instant>,
    last: Option<Confirmed>,
}

/// A confirmation: the barrier's stamp, and when the barrier started.
#[derive(Debug, Clone, Copy)]
struct Confirmed {
    at: Instant,
    stamp: ConfirmedStamp,
}

impl Confirms {
    /// Whether a confirmation may run: at most one per [`PLAN_INTERVAL`].
    fn due(&self) -> bool {
        self.at.is_none_or(|at| at.elapsed() >= PLAN_INTERVAL)
    }

    /// `view` confirmed by the last confirmation, when that confirmation
    /// started less than [`PLAN_INTERVAL`] ago and `view` still carries
    /// its stamp, by the kernel's rule ([`ConfirmedView::new`]).
    fn carried(&self, view: &Arc<PlacementView>) -> Option<ConfirmedView> {
        let last = self.last?;
        if last.at.elapsed() >= PLAN_INTERVAL {
            return None;
        }
        ConfirmedView::new(Arc::clone(view), last.stamp).ok()
    }
}

/// How an open, close or ask ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionEnd {
    Done,
    /// The group is not acted on again for one [`PLAN_INTERVAL`].
    Failed,
    /// An ask ended, with a live leader's answer or none.
    Asked(Option<GroupAnswer>),
}

/// One open, close or ask the driver runs.
struct Action {
    core: CoreId,
    task: JoinHandle<()>,
}

/// The opens, closes and asks the driver runs, one per group at a time.
struct Actions {
    running: BTreeMap<RaftGroupId, Action>,
    /// The groups whose last action failed less than [`PLAN_INTERVAL`]
    /// ago, each with when the driver learned it failed.
    failed: BTreeMap<RaftGroupId, Instant>,
    done_tx: mpsc::UnboundedSender<(RaftGroupId, ActionEnd)>,
    done: mpsc::UnboundedReceiver<(RaftGroupId, ActionEnd)>,
}

impl Actions {
    fn new() -> Self {
        let (done_tx, done) = mpsc::unbounded_channel();
        Self {
            running: BTreeMap::new(),
            failed: BTreeMap::new(),
            done_tx,
            done,
        }
    }

    /// Forgets every action that ended, and every failure older than
    /// [`PLAN_INTERVAL`].
    fn reap(&mut self, askers: &mut Askers) {
        while let Ok((group, end)) = self.done.try_recv() {
            self.end(group, end, askers);
        }
        self.failed
            .retain(|_, failed| failed.elapsed() < PLAN_INTERVAL);
    }

    /// Forgets the action of `group`, which ended by `end`, and records an
    /// ask's answer in `askers`.
    fn end(&mut self, group: RaftGroupId, end: ActionEnd, askers: &mut Askers) {
        self.running.remove(&group);
        match end {
            ActionEnd::Done => {
                self.failed.remove(&group);
            }
            ActionEnd::Failed => {
                self.failed.insert(group, Instant::now());
            }
            ActionEnd::Asked(answered) => askers.ended(group, answered),
        }
    }

    /// Whether `core` runs fewer than [`ACTIONS_PER_CORE`] actions.
    fn room(&self, core: CoreId) -> bool {
        self.running
            .values()
            .filter(|action| action.core == core)
            .count()
            < ACTIONS_PER_CORE
    }
}

impl Drop for Actions {
    /// A stopped driver stops what it runs.
    fn drop(&mut self) {
        for action in self.running.values() {
            action.task.abort();
        }
    }
}
