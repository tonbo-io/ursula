//! Scenario `identity-fence` (#768 addendum, section 7): the process
//! identity fence over a [`MetaCluster`] of three placed nodes whose runs
//! start as servers' do ([`MetaCluster::start_fenced`]): each behind a disk
//! lease of [`LEASE`], as its node of one cluster, its Raft RPCs of both
//! planes stamped with its process's incarnation and gated by its fence.
//! The recovery calls (the rejoin barriers, the probes, the genesis probe
//! and the forwarded control commands) are each process's own too: they
//! carry its incarnation, follow its routes and meet the fences at both
//! ends, as over gRPC (`ursula_raft::InProcessCaller`).
//!
//! A family run forms the cluster, appends to both data groups, and then,
//! by its seed, one of:
//!
//! - **split** (T2): under `always`, a follower's volume moves while its old
//!   process runs on. The group's leader keeps reaching the old process, the
//!   third node reaches only the new one, and the leader and the third node
//!   are cut off from each other. The leader answers linearizable reads
//!   while the new process waits out its predecessor and then elects a
//!   leader with the third node, which takes writes. Healed, the group takes
//!   writes within a bound, with no operator step.
//! - **crash during the wait** (D27): the split, with the new process
//!   crashing during its wait and restarting on the same boot.
//! - **remount under never** (T4, D23): a follower's volume moves under
//!   `never`, and its new process runs beside the old one, with the leader
//!   and the third node up, under a write load that the old process acks
//!   from its page cache until its lease ends.
//! - **lost acknowledgements under never**: the leader and a follower take
//!   appends while the third node is cut off, the follower's volume moves,
//!   and the leader is killed. The third node and the new process meet,
//!   then the leader restarts.
//! - **correlated power loss** under `always`: every node loses its power,
//!   waits out its predecessor and recovers.
//! - **the release rule broken**: a node gets a new disk while its old
//!   process keeps its own, and the operator releases the old root. Every
//!   peer reaches meta, the old process reaches no one, and one group's
//!   leader keeps reaching the old process over a pinned route while the
//!   new root binds, waits and takes part in the other group.
//! - **a new disk** (D1, D15): a node's new disk is refused while its old
//!   process runs on its own disk, for longer than a predecessor wait. Then
//!   the old disk detaches, the operator releases its root, and the new
//!   disk becomes the node, binds and waits from its binding.
//! - **replacements**: a new disk under a live id while that node's old
//!   process leads meta, then the old disk detaches while it leads, the new
//!   disk takes the id after the release, and the released disk is swapped
//!   back in and fences itself, with meta writes throughout.
//! - **a crash between meta's answer and its record** (S7(d2)): a new disk's
//!   first binding answer arrives, the process stops at the step before it
//!   records the binding and is killed there, and its next run binds again.
//!
//! Its checks:
//!
//! - I16 ([`I16`]): no data replica of a process whose fence holds it out (a
//!   predecessor wait it owes, or a binding its unbound root awaits), or
//!   whose unbound root was bound less than a predecessor wait ago by the
//!   harness's clock, opens its gate, writes a vote floor, or shows at a
//!   step an uncommitted vote other than the one it opened with. It reads
//!   each replica at every step, and reads neither meta replicas nor
//!   snapshot objects. It does not see a granted vote: a candidate that wins
//!   commits the vote with its first heartbeat, well inside a step, and the
//!   vote then reads as a leader's. No control exercises the vote clause.
//!   Under `always` a moved volume's replicas open with their gates open
//!   and need no floor, so there the gate and floor clauses judge only the
//!   empty replicas of a new disk.
//! - I17 ([`I17`]): no process counts an answer of a process of a node
//!   given after a later process of that node made a promise (granted a
//!   vote, acknowledged entries or installed a snapshot). It reads every
//!   answer a process counted, the recovery calls' included, and runs at
//!   the end of a run.
//! - I18 ([`I18`]): no linearizable read invoked after a write was
//!   acknowledged misses that write. It compares each read with its
//!   invocation, so a read invoked before a write is free to miss it.
//! - I1 ([`I1`]): every running replica reads every acknowledged write at
//!   the end.
//! - Liveness ([`RESUME`]): after the faults heal, every group takes a write
//!   within a predecessor wait and a bound.
//! - In **replacements** also the meta invariants of `placement-move` (I12
//!   and the hosting checks at every step, I10 at the end), that the node's
//!   healthy process is never refused while a new disk under its id is,
//!   and that the refused root takes no meta vote floor.
//!
//! The other scripts check neither the meta invariants nor the hosting
//! check: after a correlated power loss every node hosts what its journals
//! hold while no view shows a bootstrap. The negative controls
//! ([`Controls`]) each run one script with one defence off and fail inside
//! it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::lock::Mutex as AsyncMutex;
use openraft::ServerState;
use ursula_config::WalFsync;
use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::RegistrationStatus;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::Registered;
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_raft::JournalTuning;
use ursula_raft::RaftGroupEngine;
use ursula_raft::meta::pause::MetaPausePoint;
use ursula_raft::predecessor_wait::PREDECESSOR_WAIT;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::node_fence::FenceStatus;
use ursula_stream::sim_controls::Defence;
use ursula_stream::sim_controls::set_off;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::meta_checks::BOUND;
use super::meta_checks::MetaChecks;
use super::meta_checks::STEP;
use super::meta_checks::fail;
use super::meta_cluster::MetaCluster;
use super::meta_cluster::Zombie;
use super::meta_cluster::claim;
use super::placed_hosting::ReplicaMarks;

pub(super) const I1: &str = "acknowledged_writes_read_back";
pub(super) const I16: &str = "held_replicas_take_no_part";
pub(super) const I17: &str = "no_answer_counted_after_a_successor_promised";
pub(super) const I18: &str = "linearizable_reads_see_acknowledged_writes";
pub(super) const RESUME: &str = "fenced_cluster_writes_resume";
/// The replacements' own check: the node's healthy process is never
/// refused, and a refused root takes no meta floor.
pub(super) const REFUSED: &str = "only_the_new_root_is_refused";

/// `raft.wal.lease` of every node.
const LEASE: Duration = Duration::from_secs(5);
/// How often the disk lease's probe runs.
const PROBE: Duration = ursula_raft::PROBE_INTERVAL;
const NODES: u64 = 3;
/// How often a reader reads, and how long one read may take.
const READ_EVERY: Duration = Duration::from_millis(20);
const READ_TIMEOUT: Duration = Duration::from_millis(300);
/// How long one append may take before its outcome counts as unknown.
const APPEND_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a split keeps reading after the first write on the far side.
const READ_ON: Duration = Duration::from_secs(2);
/// How much earlier than a predecessor wait after its binding I16's clock
/// clause ends: the harness sees an answer up to a step after the node
/// does.
const BINDING_MARGIN: Duration = Duration::from_millis(100);
/// Mixed into the seed of a run's draws.
const SALT: u64 = 0x6964_656e_7469_7479;

/// Which run a scenario makes, and which defence it turns off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Controls {
    /// The family run, every defence on.
    #[default]
    Family,
    /// The split, with the successor's wait on (`true`) or off (N12).
    SuccessorWait { defended: bool },
    /// The split, with the disk lease on (`true`) or never ending (N21).
    DiskLease { defended: bool },
    /// The split with a crash during the wait, with the owed wait recorded
    /// (`true`) or not (N22).
    OwedWait { defended: bool },
    /// The remount under `never`, with the barrier after the wait (`true`)
    /// or during it (N13).
    GateAfterWait { defended: bool },
    /// The lost acknowledgements under `never`, with a run on another
    /// device read as a moved volume (`true`) or as a process crash (N20).
    RemountedRun { defended: bool },
    /// The broken release rule, with each answer's stamp judged (`true`)
    /// or every answer counted unchecked (N10).
    ResponderBinding { defended: bool },
    /// The new disk refused for longer than a wait, with its wait from its
    /// binding (`true`) or from the start of its run (N23).
    WaitFromBinding { defended: bool },
}

/// The run's defences, reset when the run ends.
struct Switched;

const SWITCHED: [Defence; 7] = [
    Defence::SuccessorWait,
    Defence::DiskLease,
    Defence::OwedWait,
    Defence::GateAfterWait,
    Defence::RemountedRun,
    Defence::ResponderBinding,
    Defence::WaitFromBinding,
];

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_off(
            Defence::SuccessorWait,
            controls == Controls::SuccessorWait { defended: false },
        );
        set_off(
            Defence::DiskLease,
            controls == Controls::DiskLease { defended: false },
        );
        set_off(
            Defence::OwedWait,
            controls == Controls::OwedWait { defended: false },
        );
        set_off(
            Defence::GateAfterWait,
            controls == Controls::GateAfterWait { defended: false },
        );
        set_off(
            Defence::RemountedRun,
            controls == Controls::RemountedRun { defended: false },
        );
        set_off(
            Defence::ResponderBinding,
            controls == Controls::ResponderBinding { defended: false },
        );
        set_off(
            Defence::WaitFromBinding,
            controls == Controls::WaitFromBinding { defended: false },
        );
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        for defence in SWITCHED {
            set_off(defence, false);
        }
    }
}

/// One linearizable read a reader completed.
#[derive(Debug, Clone)]
struct Read {
    node_id: u64,
    invoked: madsim::time::Instant,
    payload: Vec<u8>,
}

/// I16's, I17's and I18's records across a run.
#[derive(Default)]
struct FenceChecks {
    /// How many observations saw a held replica.
    held_observations: u64,
    /// Each acknowledged write, by group: when, and its payload.
    acknowledged: BTreeMap<u32, Vec<(madsim::time::Instant, Vec<u8>)>>,
    /// How many linearizable reads I18 compared.
    reads: u64,
    /// The processes seen awaiting their root's binding.
    unbound: BTreeSet<u64>,
    /// For each of them whose hold for the binding ended, when the harness
    /// first saw that: the binding answer of a bootstrapped cluster, or
    /// `None` for an answer before Bootstrap, which owes no wait.
    bindings: BTreeMap<u64, Option<madsim::time::Instant>>,
    /// The first promise of each process, by tag, read from the answers
    /// before `read_answers`.
    promised: BTreeMap<u64, madsim::time::Instant>,
    read_answers: usize,
}

impl FenceChecks {
    /// I16 at one step: every data replica of a held process shows the
    /// gate and floor it showed when it opened, before its drivers ran, and
    /// either that vote or a committed one. A vote it granted to a candidate
    /// that won is committed by then, so the vote clause sees only an
    /// uncommitted vote at the step. A held process opens its replicas while
    /// held, since a hold begins at the start of its run or, for a new
    /// disk's, their creation follows the binding. A process is held while
    /// its fence holds it out, and for a predecessor wait after the harness
    /// first saw its unbound root bound.
    fn observe(&mut self, cluster: &MetaCluster, trace: &mut SimTrace, after_event: &str) {
        let now = madsim::time::Instant::now();
        for node_id in cluster.running() {
            let Some(process) = cluster.process(node_id) else {
                continue;
            };
            let fence = cluster.data.wals[&node_id].raft_wal().fence();
            let awaits = fence.awaits_binding();
            if awaits {
                self.unbound.insert(process);
            }
            let bound_at = self.binding(cluster, node_id, process, awaits, now);
            let by_clock = bound_at.is_some_and(|at| now + BINDING_MARGIN < at + PREDECESSOR_WAIT);
            let by_fence = fence.holds_data();
            if !by_fence && !by_clock {
                continue;
            }
            for replica in cluster.hosted_replicas(node_id) {
                let now_marks = ReplicaMarks::of(&replica.raft, &replica.rejoin);
                let opened = replica.opened;
                self.held_observations = self.held_observations.saturating_add(1);
                let broke = if now_marks.gate_open && !opened.gate_open {
                    Some("opened its gate")
                } else if now_marks.floor && !opened.floor {
                    Some("wrote a vote floor")
                } else if now_marks.vote != opened.vote && !now_marks.vote.is_committed() {
                    Some("granted a vote or campaigned")
                } else {
                    None
                };
                if let Some(broke) = broke {
                    fail(
                        trace,
                        I16,
                        after_event,
                        format!(
                            "node {node_id}'s process {process} {broke} in group {} while held \
                             (by its fence: {by_fence}, within the wait after its binding at \
                             {bound_at:?}: {by_clock}): opened {opened:?}, now {now_marks:?}",
                            replica.group
                        ),
                    );
                }
            }
        }
    }

    /// When the harness first saw the hold of `process`, a process of
    /// `node_id` whose unbound root it saw await its binding, end on a
    /// binding answer of a bootstrapped cluster. `awaits` is whether the
    /// hold stands now.
    fn binding(
        &mut self,
        cluster: &MetaCluster,
        node_id: u64,
        process: u64,
        awaits: bool,
        now: madsim::time::Instant,
    ) -> Option<madsim::time::Instant> {
        if let Some(at) = self.bindings.get(&process) {
            return *at;
        }
        if awaits || !self.unbound.contains(&process) {
            return None;
        }
        let bootstrapped = matches!(
            cluster.runtimes[&node_id]
                .status_reader()
                .placement_status()
                .registration,
            RegistrationStatus::Answered(Registered::Referenced(_) | Registered::Unreferenced(_))
        );
        let at = bootstrapped.then_some(now);
        self.bindings.insert(process, at);
        at
    }

    /// Whether `process` made a promise a peer counted, reading the answers
    /// counted since the last call.
    fn promised(&mut self, cluster: &MetaCluster, process: u64) -> bool {
        let started = cluster.started_processes();
        let answers = cluster.data.answers.since(self.read_answers);
        self.read_answers = self.read_answers.saturating_add(answers.len());
        for answer in answers.iter().filter(|answer| answer.promise) {
            if let Some((tag, _)) = process_of(&started, &answer.responder) {
                self.promised.entry(tag).or_insert(answer.at);
            }
        }
        self.promised.contains_key(&process)
    }

    /// I18 over `reads` of `group`: each holds every write acknowledged
    /// before it was invoked.
    fn reads_see_writes(
        &mut self,
        trace: &mut SimTrace,
        after_event: &str,
        group: u32,
        reads: &[Read],
    ) {
        let acknowledged = self.acknowledged.get(&group).cloned().unwrap_or_default();
        for read in reads {
            self.reads = self.reads.saturating_add(1);
            if let Some((_, missed)) = acknowledged
                .iter()
                .find(|(at, payload)| *at < read.invoked && !contains(&read.payload, payload))
            {
                fail(
                    trace,
                    I18,
                    after_event,
                    format!(
                        "a linearizable read of group {group} on node {} missed the write {:?} \
                         acknowledged before it was invoked: it read {} bytes",
                        read.node_id,
                        String::from_utf8_lossy(missed),
                        read.payload.len()
                    ),
                );
            }
        }
    }
}

/// The tag and node of the process whose incarnation is `incarnation`,
/// among the processes `started` names.
fn process_of(
    started: &BTreeMap<u64, (u64, ursula_proto::placement::ProcessIncarnation)>,
    incarnation: &ursula_proto::placement::ProcessIncarnation,
) -> Option<(u64, u64)> {
    started
        .iter()
        .find(|(_, (_, seen))| seen == incarnation)
        .map(|(tag, (node, _))| (*tag, *node))
}

/// When each process first made a promise a peer counted, by tag.
fn promises(cluster: &MetaCluster) -> BTreeMap<u64, madsim::time::Instant> {
    let started = cluster.started_processes();
    let mut promised = BTreeMap::new();
    for answer in cluster.data.answers.answers() {
        if !answer.promise {
            continue;
        }
        if let Some((tag, _)) = process_of(&started, &answer.responder) {
            promised.entry(tag).or_insert(answer.at);
        }
    }
    promised
}

/// I17 over every answer a process counted: none came from a process of a
/// node after a later process of the node had made a promise.
fn answers_after_promises(cluster: &MetaCluster, trace: &mut SimTrace, after_event: &str) {
    let started = cluster.started_processes();
    let promised = promises(cluster);
    for answer in cluster.data.answers.answers() {
        let Some((tag, node)) = process_of(&started, &answer.responder) else {
            continue;
        };
        let successor = promised.iter().find(|(later, at)| {
            **later > tag
                && answer.at > **at
                && started.get(later).is_some_and(|(of, _)| *of == node)
        });
        if let Some((later, at)) = successor {
            fail(
                trace,
                I17,
                after_event,
                format!(
                    "node {:?} counted an answer of node {node}'s process {tag} at {:?}, after its \
                     successor process {later} made a promise at {at:?}",
                    answer.source, answer.at
                ),
            );
        }
    }
}

/// Whether `bytes` holds `part`.
fn contains(bytes: &[u8], part: &[u8]) -> bool {
    part.is_empty() || bytes.windows(part.len()).any(|window| window == part)
}

/// Whether `bytes` holds each of `parts`, in order.
fn holds_in_order<'a>(bytes: &[u8], parts: impl IntoIterator<Item = &'a [u8]>) -> bool {
    let mut rest = bytes;
    for part in parts {
        match rest
            .windows(part.len().max(1))
            .position(|window| window == part)
        {
            Some(at) => {
                rest = rest
                    .get(at.saturating_add(part.len())..)
                    .unwrap_or_default()
            }
            None => return false,
        }
    }
    true
}

/// Reads `group` linearizably on `node_id`'s `engine` every
/// [`READ_EVERY`] until aborted, keeping each read that completed.
fn spawn_reader(
    engine: Arc<AsyncMutex<RaftGroupEngine>>,
    node_id: u64,
    group: u32,
    reads: Arc<Mutex<Vec<Read>>>,
) -> madsim::task::JoinHandle<()> {
    madsim::task::spawn(async move {
        loop {
            let invoked = madsim::time::Instant::now();
            let read = madsim::time::timeout(READ_TIMEOUT, async {
                engine
                    .lock()
                    .await
                    .read_stream(
                        ReadStreamRequest {
                            stream_id: group_stream(group),
                            offset: 0,
                            max_len: 1 << 20,
                            now_ms: 0,
                            leader_only: true,
                            read_index: None,
                            follower: FollowerRead::Prefix,
                        },
                        group_placement(group),
                    )
                    .await
            })
            .await;
            if let Ok(Ok(read)) = read {
                reads
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .push(Read {
                        node_id,
                        invoked,
                        payload: read.payload,
                    });
            }
            madsim::time::sleep(READ_EVERY).await;
        }
    })
}

/// The reads a reader completed.
fn completed(reads: &Arc<Mutex<Vec<Read>>>) -> Vec<Read> {
    reads
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

/// The three nodes of a split: `leader` led the group, `moved`'s volume
/// moves, `far` is the third.
#[derive(Debug, Clone, Copy)]
struct Roles {
    group: u32,
    leader: u64,
    moved: u64,
    far: u64,
}

struct Run {
    cluster: MetaCluster,
    trace: SimTrace,
    meta: MetaChecks,
    fence: FenceChecks,
    draws: SplitMix64,
    seed: u64,
    /// Each payload appended, by group, of which the acknowledged ones are
    /// in `fence.acknowledged`.
    appended: BTreeMap<u32, u64>,
    /// Whether every step also runs the meta checks ([`MetaChecks::observe`]).
    meta_checked: bool,
}

impl Run {
    async fn start(seed: u64, fsync: WalFsync) -> Self {
        let mut trace = SimTrace::default();
        trace.push(SimEvent::ClusterBuilt { seed });
        let claims = (1..=NODES)
            .map(|node_id| (node_id, claim(NODES, &[1, 2, 3])))
            .collect();
        let cluster = MetaCluster::start_fenced(
            &format!("identity-fence-{seed}"),
            JournalTuning::new(fsync),
            NODES,
            claims,
            LEASE,
        )
        .await;
        let mut run = Self {
            cluster,
            trace,
            meta: MetaChecks::default(),
            fence: FenceChecks::default(),
            draws: SplitMix64::new(seed ^ SALT),
            seed,
            appended: BTreeMap::new(),
            meta_checked: false,
        };
        run.step("cluster_started");
        run.cluster.groups_form(&mut run.trace, "formed").await;
        let nodes: Vec<u64> = (1..=NODES).collect();
        run.meta
            .registered_nodes_learn(&run.cluster, &mut run.trace, "formed", &nodes)
            .await;
        run.meta
            .bootstrapped(&run.cluster, &mut run.trace, "formed", &nodes)
            .await;
        for group in JOURNAL_GROUPS {
            run.resume(group, "formed").await;
        }
        run
    }

    /// Records `phase` and runs the checks of a step.
    fn step(&mut self, phase: &str) {
        self.trace.push(SimEvent::FaultApplied {
            phase: phase.to_owned(),
        });
        self.observe(phase);
    }

    fn observe(&mut self, phase: &str) {
        self.fence.observe(&self.cluster, &mut self.trace, phase);
        if self.meta_checked {
            self.meta.observe(&self.cluster, &mut self.trace, phase);
        }
    }

    /// Waits `wait`, running the checks every step.
    async fn watch(&mut self, phase: &str, wait: Duration) {
        let deadline = madsim::time::Instant::now() + wait;
        while madsim::time::Instant::now() < deadline {
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// The node among `among` whose replica of `group` leads, if one does.
    fn leader(&self, group: u32, among: &[u64]) -> Option<u64> {
        among.iter().copied().find(|node_id| {
            self.cluster
                .hosted_replicas(*node_id)
                .iter()
                .any(|replica| {
                    replica.group == group
                        && openraft::rt::WatchReceiver::borrow_watched(&replica.raft.metrics())
                            .state
                            == ServerState::Leader
                })
        })
    }

    /// Appends one payload to `group` through `node_id`'s replica, and
    /// answers whether it was acknowledged. One that times out may still
    /// commit: it is never expected.
    async fn append_on(&mut self, group: u32, node_id: u64) -> bool {
        let Some(engine) = self.cluster.hosted_engine(node_id, group) else {
            return false;
        };
        let count = self.appended.entry(group).or_default();
        let payload = format!("g{group}-{count};").into_bytes();
        *count = count.saturating_add(1);
        let appended = madsim::time::timeout(APPEND_TIMEOUT, async {
            engine
                .lock()
                .await
                .append(
                    AppendRequest::from_bytes(group_stream(group), payload.clone()),
                    group_placement(group),
                    ColdWriteAdmission::default(),
                )
                .await
        })
        .await;
        if !matches!(appended, Ok(Ok(_))) {
            return false;
        }
        self.fence
            .acknowledged
            .entry(group)
            .or_default()
            .push((madsim::time::Instant::now(), payload));
        true
    }

    /// Appends one payload to `group` through whichever of `among` leads it,
    /// if one does.
    async fn append_through_leader(&mut self, group: u32, among: &[u64]) -> bool {
        match self.leader(group, among) {
            Some(leader) => self.append_on(group, leader).await,
            None => false,
        }
    }

    /// Liveness: `group` takes a write through a leader among the running
    /// nodes within a predecessor wait and the bound.
    async fn resume(&mut self, group: u32, phase: &str) {
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + BOUND;
        loop {
            let running = self.cluster.running();
            if self.append_through_leader(group, &running).await {
                return;
            }
            if madsim::time::Instant::now() >= deadline {
                let nodes = self.describe(group);
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("group {group} took no write within the bound: {nodes}"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// I1: every running node's replica of every group reads every
    /// acknowledged write, in order, within the bound.
    async fn reads_back(&mut self, phase: &str) {
        for group in JOURNAL_GROUPS {
            let expected: Vec<Vec<u8>> = self
                .fence
                .acknowledged
                .get(&group)
                .map(|writes| writes.iter().map(|(_, payload)| payload.clone()).collect())
                .unwrap_or_default();
            let deadline = madsim::time::Instant::now() + BOUND;
            for node_id in self.cluster.running() {
                loop {
                    let read = match self.cluster.hosted_engine(node_id, group) {
                        Some(engine) => engine
                            .lock()
                            .await
                            .sim_read_local_stream(
                                ReadStreamRequest {
                                    stream_id: group_stream(group),
                                    offset: 0,
                                    max_len: 1 << 20,
                                    now_ms: 0,
                                    leader_only: false,
                                    read_index: None,
                                    follower: FollowerRead::Prefix,
                                },
                                group_placement(group),
                            )
                            .await
                            .map(|read| read.payload)
                            .ok(),
                        None => None,
                    };
                    if read.as_deref().is_some_and(|bytes| {
                        holds_in_order(bytes, expected.iter().map(Vec::as_slice))
                    }) {
                        break;
                    }
                    if madsim::time::Instant::now() >= deadline {
                        let nodes = self.describe(group);
                        fail(
                            &mut self.trace,
                            I1,
                            phase,
                            format!(
                                "node {node_id} does not read group {group}'s {} acknowledged \
                                 writes, the last {:?}: it reads {:?}; {}",
                                expected.len(),
                                expected
                                    .last()
                                    .map(|last| String::from_utf8_lossy(last).into_owned()),
                                read.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
                                nodes
                            ),
                        );
                    }
                    madsim::time::sleep(STEP).await;
                }
            }
        }
        self.step(phase);
    }

    /// What each running node's replica of `group` shows, for a failure.
    fn describe(&self, group: u32) -> String {
        self.cluster
            .running()
            .into_iter()
            .map(|node_id| {
                let held = self.cluster.data.wals[&node_id]
                    .raft_wal()
                    .fence()
                    .holds_data();
                let replica = self
                    .cluster
                    .hosted_replicas(node_id)
                    .into_iter()
                    .find(|replica| replica.group == group)
                    .map(|replica| {
                        let metrics =
                            openraft::rt::WatchReceiver::borrow_watched(&replica.raft.metrics())
                                .clone();
                        format!(
                            "{:?} vote {} leader {:?} last {:?} applied {:?} gate {:?} floor {}",
                            metrics.state,
                            metrics.vote,
                            metrics.current_leader,
                            metrics.last_log_index,
                            metrics.last_applied.map(|id| id.index),
                            replica.rejoin.status(),
                            replica.rejoin.holds_vote_floor()
                        )
                    });
                format!(
                    "node {node_id} (process {:?}, held {held}): {replica:?}",
                    self.cluster.process(node_id)
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// A drawn group.
    fn draw_group(&mut self) -> u32 {
        JOURNAL_GROUPS
            .get(usize::try_from(self.draws.next_bounded(2)).unwrap_or(0))
            .copied()
            .unwrap_or(0)
    }

    /// The node whose replica of `group` leads within the bound.
    async fn wait_leader(&mut self, group: u32, phase: &str) -> u64 {
        let nodes: Vec<u64> = (1..=NODES).collect();
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            if let Some(leader) = self.leader(group, &nodes) {
                return leader;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("group {group} has no leader"),
                );
            }
            madsim::time::sleep(STEP).await;
        }
    }

    /// The roles of a split of a drawn group, by who leads it now.
    async fn roles(&mut self, phase: &str) -> Roles {
        let group = self.draw_group();
        let leader = self.wait_leader(group, phase).await;
        let others: Vec<u64> = (1..=NODES).filter(|node| *node != leader).collect();
        let pick = usize::try_from(self.draws.next_bounded(2)).unwrap_or(0);
        let moved = others.get(pick).copied().unwrap_or(leader);
        let far = others
            .iter()
            .copied()
            .find(|node| *node != moved)
            .unwrap_or(leader);
        Roles {
            group,
            leader,
            moved,
            far,
        }
    }

    /// Moves `group`'s leadership to `to` within the bound.
    async fn lead(&mut self, group: u32, to: u64, phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            let leader = self.wait_leader(group, phase).await;
            if leader == to {
                return;
            }
            if let Some(replica) = self
                .cluster
                .hosted_replicas(leader)
                .into_iter()
                .find(|replica| replica.group == group)
                && let Err(error) = replica.raft.trigger().transfer_leader(to).await
            {
                tracing::info!(%error, group, to, "a leadership transfer did not start");
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("group {group}'s leadership never moved to node {to}"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// The node whose meta replica leads, within the bound.
    async fn wait_meta_leader(&mut self, phase: &str) -> u64 {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            if let Some(leader) = self.cluster.meta_leader() {
                return leader;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    "the meta group has no leader".to_owned(),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// The operator releases `node_id`'s root `root` through `via`'s meta
    /// handle (`ursulactl node replace-disk`), within the bound.
    async fn release(&mut self, node_id: u64, root: RootId, via: u64, phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            let answer = self
                .cluster
                .handle(via)
                .submit(ControlCommand::ReleaseRoot {
                    node: NodeId::new(node_id),
                    root,
                })
                .await;
            match answer {
                Ok(ControlApplied::RootReleased(_)) => break,
                Ok(other) => fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("releasing node {node_id}'s root answered {other:?}"),
                ),
                Err(error) if madsim::time::Instant::now() >= deadline => fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("node {node_id}'s root was not released within the bound: {error}"),
                ),
                Err(_retried) => {
                    madsim::time::sleep(STEP).await;
                    self.observe(phase);
                }
            }
        }
        self.step(phase);
    }

    /// Waits until the view of every node of `viewers` gives `node_id` the
    /// slot `slot`, within the bound.
    async fn views_show(&mut self, viewers: &[u64], node_id: u64, slot: RootSlot, phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            let lagging: Vec<u64> = viewers
                .iter()
                .copied()
                .filter(|viewer| self.cluster.slot_in_view(*viewer, node_id) != Some(slot))
                .collect();
            if lagging.is_empty() {
                return;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!(
                        "the views of nodes {lagging:?} do not give node {node_id} the slot \
                         {slot:?}"
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// Sleeps until `offset` after the next successful probe of `node_id`'s
    /// current run, read from its fence, so a volume that moves then leaves
    /// its old process `PROBE` less `offset` until its next probe fails.
    async fn after_probe(&mut self, node_id: u64, offset: Duration) {
        let fence = self.cluster.data.wals[&node_id].raft_wal().fence();
        let last = fence.last_success_start();
        let deadline = madsim::time::Instant::now() + PROBE * 3;
        while fence.last_success_start() == last {
            assert!(
                madsim::time::Instant::now() < deadline,
                "node {node_id}'s disk lease probe did not succeed within {:?}",
                PROBE * 3
            );
            madsim::time::sleep(Duration::from_millis(1)).await;
        }
        madsim::time::sleep(offset).await;
    }

    /// The split (T2): see the module docs. `crash` restarts the new
    /// process during its wait (D27). `offset` is how long after the old
    /// process's last probe its volume moves.
    async fn split(&mut self, offset: Duration, crash: bool) {
        const PHASE: &str = "split";
        let roles = self.roles(PHASE).await;
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("{PHASE}: {roles:?}"),
        });
        let Roles {
            group,
            leader,
            moved,
            far,
        } = roles;
        if !self.append_on(group, leader).await {
            self.resume(group, PHASE).await;
        }
        let leader_process = self.cluster.process(leader).expect("the leader runs");
        self.after_probe(moved, offset).await;
        self.step(PHASE);
        let zombie = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[leader_process],
        );
        self.cluster.cut_route(zombie.process, far);
        self.cluster
            .data
            .policy
            .partition_bidirectional(leader, far);
        self.start_cut_off(moved, leader).await;
        if crash {
            madsim::time::sleep(Duration::from_millis(100)).await;
            self.cluster.stop_node(moved).await;
            self.start_cut_off(moved, leader).await;
        }
        self.step(PHASE);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let engine = self
            .cluster
            .hosted_engine(leader, group)
            .expect("the old leader runs the group");
        let reader = spawn_reader(engine, leader, group, Arc::clone(&reads));
        // The far side elects once the new process waited out its
        // predecessor, and takes writes.
        self.far_side_writes(group, &[far, moved], PHASE).await;
        reader.abort();
        let reads = completed(&reads);
        if reads.is_empty() {
            fail(
                &mut self.trace,
                I18,
                PHASE,
                format!(
                    "the old leader {leader} completed no linearizable read of group {group} \
                     during the split, so I18 judged nothing"
                ),
            );
        }
        self.fence
            .reads_see_writes(&mut self.trace, PHASE, group, &reads);
        self.heal(vec![zombie]).await;
        self.resume(group, "split_healed").await;
        self.reads_back("split_reads").await;
    }

    /// Writes to `group` through a leader among `far` until [`READ_ON`]
    /// after the first write it took, or a predecessor wait and the bound.
    async fn far_side_writes(&mut self, group: u32, far: &[u64], phase: &str) {
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + BOUND;
        let mut written_at = None;
        loop {
            let now = madsim::time::Instant::now();
            if written_at.is_some_and(|at| now >= at + READ_ON) || now >= deadline {
                break;
            }
            if self.append_through_leader(group, far).await {
                written_at.get_or_insert(now);
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// Starts `node_id`'s next process, whose RPCs to `cut_from` are cut.
    async fn start_cut_off(&mut self, node_id: u64, cut_from: u64) {
        let process = self.cluster.next_process().expect("a fenced cluster");
        self.cluster.cut_route(process, cut_from);
        self.cluster.start_node(node_id).await;
    }

    /// Starts `node_id`'s next process on a root the meta group never bound
    /// it to, which I16's clock clause holds from its binding.
    async fn start_unbound(&mut self, node_id: u64) {
        self.cluster.start_node(node_id).await;
        if let Some(process) = self.cluster.process(node_id) {
            self.fence.unbound.insert(process);
        }
    }

    /// Ends every partition and route, and kills `zombies`.
    async fn heal(&mut self, zombies: Vec<Zombie>) {
        self.cluster.clear_routes();
        self.cluster.data.policy.clear();
        for zombie in zombies {
            drop(zombie.kill().await);
        }
        self.step("healed");
    }

    /// The remount under `never` (T4, D23): a follower's volume moves and
    /// its new process opens `Recovering` beside the old one, whose disk
    /// lease ends at its next probe. The leader takes writes all along, so
    /// the old process acks appends from its page cache until its lease
    /// ends. The new process writes no floor and opens no gate during its
    /// wait (I16), then rejoins with every acknowledged write (I1).
    async fn remount(&mut self) {
        const PHASE: &str = "remount";
        let Roles {
            group,
            leader,
            moved,
            ..
        } = self.roles(PHASE).await;
        if !self.append_on(group, leader).await {
            self.resume(group, PHASE).await;
        }
        let leader_process = self.cluster.process(leader).expect("the leader runs");
        let zombie = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[leader_process],
        );
        self.cluster.start_node(moved).await;
        self.step(PHASE);
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT / 2;
        while madsim::time::Instant::now() < deadline {
            self.append_on(group, leader).await;
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        self.heal(vec![zombie]).await;
        self.watch("remount_waited", PREDECESSOR_WAIT).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "remount_healed").await;
        }
        self.reads_back("remount_reads").await;
    }

    /// The lost acknowledgements under `never`: the leader and the
    /// follower whose volume then moves take appends while the third node
    /// is cut off, and the leader is killed. The new process reads its
    /// moved volume `Remounted` and waits, so the third node and it elect
    /// nobody until the leader restarts with every acknowledged write (I1).
    async fn lost_acknowledgements(&mut self) {
        const PHASE: &str = "lost_acknowledgements";
        let Roles {
            group,
            leader,
            moved,
            far,
        } = self.roles(PHASE).await;
        self.cluster
            .data
            .policy
            .partition_bidirectional(leader, far);
        for _ in 0..3 {
            if !self.append_on(group, leader).await {
                break;
            }
        }
        self.step(PHASE);
        let zombie = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[],
        );
        drop(zombie.kill().await);
        self.cluster.stop_node(leader).await;
        self.cluster.data.policy.clear();
        self.cluster.start_node(moved).await;
        self.step(PHASE);
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT / 3;
        while madsim::time::Instant::now() < deadline {
            // A write the far side takes now replaces what the killed
            // leader holds.
            self.append_through_leader(group, &[far, moved]).await;
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        self.cluster.start_node(leader).await;
        self.step("leader_restarted");
        for group in JOURNAL_GROUPS {
            self.resume(group, "leader_restarted").await;
        }
        self.reads_back("lost_acknowledgements_reads").await;
    }

    /// A correlated power loss under `always`: every node waits out its
    /// predecessor, holding its replicas out (I16), then every group takes
    /// writes again and keeps what it acknowledged (I1).
    async fn power_loss(&mut self) {
        const PHASE: &str = "power_loss";
        let nodes: Vec<u64> = (1..=NODES).collect();
        for node_id in &nodes {
            self.cluster.stop_node(*node_id).await;
        }
        for node_id in &nodes {
            self.cluster.data.wals[node_id]
                .power_loss_losing_unsynced()
                .await;
        }
        for node_id in &nodes {
            self.cluster.start_node(*node_id).await;
        }
        self.step(PHASE);
        for group in JOURNAL_GROUPS {
            self.resume(group, PHASE).await;
        }
        self.reads_back("power_loss_reads").await;
    }

    /// The release rule broken, every peer reaching meta (I17): see the
    /// module docs. `meta` leads the meta group and `group_b`, `leader`
    /// leads `group_a`, and `replaced` gets a new disk while its old process
    /// keeps its own. `leader` keeps reaching the old process in `group_a`
    /// and reaches the new root nowhere, and the new root never reaches
    /// `leader`, so `leader` learns of the replacement only from its view.
    async fn release_broken(&mut self) {
        const PHASE: &str = "release_broken";
        let meta = self.wait_meta_leader(PHASE).await;
        let others: Vec<u64> = (1..=NODES).filter(|node| *node != meta).collect();
        let pick = usize::try_from(self.draws.next_bounded(2)).unwrap_or(0);
        let leader = others.get(pick).copied().unwrap_or(meta);
        let replaced = others
            .iter()
            .copied()
            .find(|node| *node != leader)
            .unwrap_or(meta);
        let group_a = self.draw_group();
        let group_b = JOURNAL_GROUPS
            .iter()
            .copied()
            .find(|group| *group != group_a)
            .unwrap_or(group_a);
        self.trace.push(SimEvent::FaultApplied {
            phase: format!(
                "{PHASE}: meta {meta}, leader {leader} of {group_a}, replaced {replaced}, {group_b} \
                 led by meta"
            ),
        });
        self.lead(group_a, leader, PHASE).await;
        self.lead(group_b, meta, PHASE).await;
        let leader_process = self.cluster.process(leader).expect("the leader runs");
        // The leader's group_a RPCs to the replaced node keep reaching its old
        // process, and its other RPCs there reach no process of it.
        self.cluster
            .pin_group_route(group_a, leader_process, replaced);
        self.cluster
            .cut_group_route(group_b, leader_process, replaced);
        self.cluster.cut_meta_route(leader_process, replaced);
        let zombie = self.cluster.replace_disk(
            replaced,
            &format!("identity-fence-{}-replaced-{replaced}", self.seed),
        );
        let old_root = zombie.fence_root();
        // The old process reaches no one, so it never learns of the release.
        for peer in [leader, meta] {
            self.cluster.cut_route(zombie.process, peer);
        }
        // The new root never reaches the leader, so the leader never hears
        // of it but through its view.
        let new_process = self.cluster.next_process().expect("a fenced cluster");
        self.cluster.cut_route(new_process, leader);
        self.step(PHASE);
        self.release(replaced, old_root, meta, PHASE).await;
        self.cluster.start_node(replaced).await;
        self.step(PHASE);
        // The new root binds, waits out its predecessor and joins group_b
        // through the meta node, which counts its promises. The leader keeps
        // writing to group_a all along, its RPCs to the replaced node
        // reaching the old process.
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + BOUND;
        loop {
            self.append_on(group_a, leader).await;
            self.append_through_leader(group_b, &[meta, replaced]).await;
            if self.fence.promised(&self.cluster, new_process) {
                break;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    I17,
                    PHASE,
                    format!(
                        "node {replaced}'s new process {new_process} made no promise within the \
                         bound, so I17 judged nothing"
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        let deadline = madsim::time::Instant::now() + READ_ON;
        while madsim::time::Instant::now() < deadline {
            self.append_on(group_a, leader).await;
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        answers_after_promises(&self.cluster, &mut self.trace, PHASE);
        self.heal(vec![zombie]).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "release_broken_healed").await;
        }
        self.reads_back("release_broken_reads").await;
    }

    /// Waits until `stranger`'s registration is refused for the slot of
    /// its node's running process, within the bound, checking that the
    /// running process is never refused.
    async fn refused(&mut self, stranger: &Zombie, node_id: u64, phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        while !matches!(
            stranger.registration(),
            RegistrationStatus::RootRefused { .. }
        ) {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    REFUSED,
                    phase,
                    format!(
                        "node {node_id}'s new disk was not refused within the bound: {:?}",
                        stranger.registration()
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
            self.healthy(phase);
        }
    }

    /// The healthy processes are never refused: every running node's
    /// registration stands answered and its fence admits.
    fn healthy(&mut self, phase: &str) {
        for node_id in self.cluster.running() {
            let registration = self.cluster.runtimes[&node_id]
                .status_reader()
                .placement_status()
                .registration;
            let status = self.cluster.data.wals[&node_id]
                .raft_wal()
                .fence()
                .status(ursula_runtime::node_fence::fence_now());
            let refused = matches!(
                registration,
                RegistrationStatus::RootRefused { .. } | RegistrationStatus::Refused(_)
            );
            if refused || status == FenceStatus::Superseded {
                fail(
                    &mut self.trace,
                    REFUSED,
                    phase,
                    format!(
                        "node {node_id}'s running process was refused beside a new disk under \
                         its id: {registration:?}, fence {status:?}"
                    ),
                );
            }
        }
    }

    /// A refused root's meta replica holds no vote floor
    /// (`fence_holds_barrier`): it takes no meta floor before meta answered
    /// its registration, and none while refused.
    fn no_meta_floor(&mut self, stranger: &Zombie, node_id: u64, phase: &str) {
        let floor = stranger
            .registry()
            .meta()
            .and_then(|meta| meta.recovery_gate().map(|gate| gate.holds_vote_floor()));
        if floor == Some(true) {
            fail(
                &mut self.trace,
                REFUSED,
                phase,
                format!(
                    "node {node_id}'s new disk took a meta vote floor while its registration was \
                     {:?}",
                    stranger.registration()
                ),
            );
        }
    }

    /// A new disk (D1, D15, N23): see the module docs. The old process's
    /// disk detaches only once the new root was refused for longer than a
    /// predecessor wait, the operator releases the old root at once, and
    /// the new root takes part only a predecessor wait after its binding
    /// (I16).
    async fn new_disk(&mut self) {
        const PHASE: &str = "new_disk";
        let Roles {
            group,
            leader,
            moved,
            ..
        } = self.roles(PHASE).await;
        if !self.append_on(group, leader).await {
            self.resume(group, PHASE).await;
        }
        let stranger = self
            .cluster
            .start_stranger(moved, &format!("identity-fence-{}-new-{moved}", self.seed))
            .await;
        self.fence.unbound.insert(stranger.process);
        self.refused(&stranger, moved, PHASE).await;
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + PROBE;
        while madsim::time::Instant::now() < deadline {
            self.append_on(group, leader).await;
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
            self.healthy(PHASE);
        }
        let leader_process = self.cluster.process(leader).expect("the leader runs");
        let zombie = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[leader_process],
        );
        let old_root = zombie.fence_root();
        // Reachable from now on, and still refused until the release.
        self.cluster.promote(moved, stranger);
        self.step(PHASE);
        self.release(moved, old_root, leader, PHASE).await;
        self.watch(PHASE, PREDECESSOR_WAIT + PROBE).await;
        self.heal(vec![zombie]).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "new_disk_healed").await;
        }
        self.reads_back("new_disk_reads").await;
    }

    /// Replacements, with meta writes throughout: see the module docs.
    async fn replacements(&mut self) {
        const PHASE: &str = "replacements";
        self.meta_checked = true;
        let replaced = self.wait_meta_leader(PHASE).await;
        let others: Vec<u64> = (1..=NODES).filter(|node| *node != replaced).collect();
        self.meta.write_limits(&self.cluster, &others, 2).await;
        self.step(PHASE);
        // An empty disk under a live id, which leads meta: refused, and
        // refused again after a restart, while the node's process is never
        // refused.
        let stranger = self
            .cluster
            .start_stranger(
                replaced,
                &format!("identity-fence-{}-new-{replaced}", self.seed),
            )
            .await;
        self.refused(&stranger, replaced, PHASE).await;
        self.no_meta_floor(&stranger, replaced, PHASE);
        let disk = stranger.kill().await;
        let stranger = self.cluster.start_stranger_on(replaced, disk).await;
        self.refused(&stranger, replaced, PHASE).await;
        let deadline = madsim::time::Instant::now() + BOUND / 4;
        while madsim::time::Instant::now() < deadline {
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
            self.healthy(PHASE);
            self.no_meta_floor(&stranger, replaced, PHASE);
        }
        // The meta leader's disk detaches while it leads, meta elects
        // another leader and takes writes, and the operator releases the
        // old root.
        let zombie = self.cluster.move_volume(
            replaced,
            &format!("identity-fence-{}-moved-{replaced}", self.seed),
            &[],
        );
        let old_root = zombie.fence_root();
        let old_disk = self.cluster.data.wals[&replaced].clone();
        self.meta.write_limits(&self.cluster, &others, 2).await;
        self.fence.unbound.insert(stranger.process);
        self.cluster.promote(replaced, stranger);
        self.step(PHASE);
        let via = others.first().copied().unwrap_or(replaced);
        self.release(replaced, old_root, via, PHASE).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, PHASE).await;
        }
        self.meta.write_limits(&self.cluster, &others, 2).await;
        drop(zombie.kill().await);
        // The released disk swapped back in: its root holds state, so the
        // release fences it, and the other nodes are never refused.
        self.cluster.stop_node(replaced).await;
        let new_disk = self.cluster.data.wals[&replaced].clone();
        self.cluster.data.wals.insert(replaced, old_disk);
        self.cluster.start_node(replaced).await;
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            let status = self.cluster.data.wals[&replaced]
                .raft_wal()
                .fence()
                .status(ursula_runtime::node_fence::fence_now());
            if status == FenceStatus::Superseded {
                break;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    REFUSED,
                    PHASE,
                    format!(
                        "node {replaced}'s released disk, swapped back in, did not fence itself: \
                         {status:?}"
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        let swapped = self.cluster.retire(replaced);
        drop(swapped.kill().await);
        for node_id in &others {
            let registration = self.cluster.runtimes[node_id]
                .status_reader()
                .placement_status()
                .registration;
            if !matches!(registration, RegistrationStatus::Answered(_)) {
                fail(
                    &mut self.trace,
                    REFUSED,
                    PHASE,
                    format!("node {node_id} was refused beside the swapped disk: {registration:?}"),
                );
            }
        }
        self.cluster.data.wals.insert(replaced, new_disk);
        self.cluster.start_node(replaced).await;
        self.step("replacements_restored");
        for group in JOURNAL_GROUPS {
            self.resume(group, "replacements_restored").await;
        }
        let nodes: Vec<u64> = (1..=NODES).collect();
        self.meta
            .acknowledged_writes_survive(
                &self.cluster,
                &mut self.trace,
                "replacements_restored",
                &nodes,
            )
            .await;
        self.reads_back("replacements_reads").await;
    }

    /// A crash between meta's answer and its record (S7(d2)): see the
    /// module docs. The first run's binding is never recorded, so its next
    /// run is unbound again, is answered again and waits from that answer
    /// (I16).
    async fn registration_crash(&mut self) {
        const PHASE: &str = "registration_crash";
        let Roles {
            group,
            leader,
            moved,
            far,
        } = self.roles(PHASE).await;
        if !self.append_on(group, leader).await {
            self.resume(group, PHASE).await;
        }
        let zombie = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[],
        );
        let old_root = zombie.fence_root();
        drop(zombie.kill().await);
        self.release(moved, old_root, leader, PHASE).await;
        self.cluster
            .new_fenced_disk(moved, &format!("identity-fence-{}-new-{moved}", self.seed));
        self.cluster
            .pause_next_start(MetaPausePoint::RegistrationAnswered);
        self.start_unbound(moved).await;
        let new_root = self.cluster.data.wals[&moved].raft_wal().incarnation().root;
        self.step(PHASE);
        // Meta bound the new root, and the process stopped before it
        // recorded the binding: its data stays held.
        self.views_show(&[leader, far], moved, RootSlot::Bound(new_root), PHASE)
            .await;
        self.watch(PHASE, STEP * 10).await;
        let registration = self.cluster.runtimes[&moved]
            .status_reader()
            .placement_status()
            .registration;
        let fence = self.cluster.data.wals[&moved].raft_wal().fence();
        if matches!(registration, RegistrationStatus::Answered(_)) || !fence.awaits_binding() {
            fail(
                &mut self.trace,
                I16,
                PHASE,
                format!(
                    "node {moved}'s new root was released before it recorded its binding: \
                     {registration:?}, awaits binding {}",
                    fence.awaits_binding()
                ),
            );
        }
        self.cluster.stop_node(moved).await;
        self.start_unbound(moved).await;
        self.step("registration_crash_restarted");
        self.watch("registration_crash_restarted", PREDECESSOR_WAIT + PROBE)
            .await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "registration_crash_restarted").await;
        }
        self.reads_back("registration_crash_reads").await;
    }

    /// Ends the run: I17 over the whole run, and I16 fails when no step
    /// saw a held replica, so that every run's I16 judged something.
    fn finish(mut self) -> ThreeNodeRaftSimOutcome {
        answers_after_promises(&self.cluster, &mut self.trace, "finished");
        if self.fence.held_observations == 0 {
            fail(
                &mut self.trace,
                I16,
                "finished",
                "no step saw a held replica, so I16 judged nothing".to_owned(),
            );
        }
        self.trace.push(SimEvent::IdentityFenceHeld {
            held_observations: self.fence.held_observations,
            linearizable_reads: self.fence.reads,
        });
        ThreeNodeRaftSimOutcome {
            seed: self.seed,
            leader_id: 0,
            target_node_id: None,
            appended_log_index: 0,
            trace: self.trace,
        }
    }
}

/// How long after the old process's last probe its volume moves in a
/// scripted split: as early as can be, so its lease lasts the longest.
const SCRIPTED_OFFSET: Duration = Duration::from_millis(1);

pub(super) async fn run_identity_fence_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let seed = config.seed;
    let _switched = Switched::apply(controls);
    match controls {
        Controls::Family => family(seed).await,
        Controls::SuccessorWait { .. } | Controls::DiskLease { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.split(SCRIPTED_OFFSET, false).await;
            run.finish()
        }
        Controls::OwedWait { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.split(SCRIPTED_OFFSET, true).await;
            run.finish()
        }
        Controls::GateAfterWait { .. } => {
            let mut run = Run::start(seed, WalFsync::Never).await;
            run.remount().await;
            run.finish()
        }
        Controls::RemountedRun { .. } => {
            let mut run = Run::start(seed, WalFsync::Never).await;
            run.lost_acknowledgements().await;
            run.finish()
        }
        Controls::ResponderBinding { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.release_broken().await;
            run.finish()
        }
        Controls::WaitFromBinding { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.new_disk().await;
            run.finish()
        }
    }
}

/// The family run of `seed`: one script per seed of the family's ten.
async fn family(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut draws = SplitMix64::new(seed ^ SALT ^ 1);
    let offset = Duration::from_millis(draws.next_bounded(1000));
    let fsync = match seed % 10 {
        2 | 3 => WalFsync::Never,
        _ => WalFsync::Always,
    };
    let mut run = Run::start(seed, fsync).await;
    match seed % 10 {
        0 => run.split(offset, false).await,
        1 => run.split(offset, true).await,
        2 => run.remount().await,
        3 => run.lost_acknowledgements().await,
        4 => run.power_loss().await,
        5 => run.release_broken().await,
        6 => run.new_disk().await,
        7 => run.replacements().await,
        8 => run.registration_crash().await,
        _ => run.split(offset, false).await,
    }
    run.finish()
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::I1;
    use super::I16;
    use super::I17;
    use super::I18;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimReport;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::IdentityFence;
    /// The seeds whose family runs the replay covers: a split and a crash
    /// during the wait.
    const REPLAYED: std::ops::RangeInclusive<u64> = 1110..=1111;
    /// The seed every scripted run uses.
    const SCRIPTED: u64 = 1110;

    fn run(seed: u64, controls: Controls) -> SimReport {
        ThreeNodeRaftSim::run_identity_fence_with_controls_report(
            ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug())),
            controls,
        )
    }

    /// The run ended held, with this many held observations and reads.
    fn held(report: &SimReport) -> Option<(u64, u64)> {
        report
            .outcome
            .trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::IdentityFenceHeld {
                    held_observations,
                    linearizable_reads,
                } => Some((*held_observations, *linearizable_reads)),
                _ => None,
            })
    }

    /// The family's seeds hold the same way twice per seed. Each saw a
    /// held replica, which every run's end checks too.
    #[test]
    fn identity_fence_seeds_replay() {
        let _guard = sim_test_guard();
        for seed in REPLAYED {
            let first = run(seed, Controls::Family);
            let second = run(seed, Controls::Family);
            assert_eq!(first, second, "seed {seed}");
            assert_eq!(first.scenario, SCENARIO);
            let (held_observations, _) = held(&first).expect("the run ends held");
            assert!(held_observations > 0, "seed {seed}: no replica was held");
        }
    }

    /// The defended script passes, and the one with `off` fails `invariant`
    /// in the script's own `phase`, not while the cluster forms.
    fn assert_caught(defended: Controls, off: Controls, invariant: &str, phase: &str) -> String {
        assert!(
            held(&run(SCRIPTED, defended)).is_some(),
            "{defended:?} passes"
        );
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| run(SCRIPTED, off));
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{off:?} left the run passing");
        SimTrace::last_recorded()
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::InvariantFailed {
                    invariant: failed,
                    after_event,
                    message,
                } => {
                    assert_eq!(
                        failed, invariant,
                        "{off:?} failed another invariant: {message}"
                    );
                    assert_eq!(
                        after_event, phase,
                        "{off:?} failed outside its script: {message}"
                    );
                    Some(message.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{off:?} failed no invariant"))
    }

    /// A successor that owes no wait elects beside its old process, which
    /// still confirms the old leader's reads (N12).
    #[test]
    fn negative_control_successor_waits() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::SuccessorWait { defended: true },
            Controls::SuccessorWait { defended: false },
            I18,
            "split",
        );
        assert!(message.contains("missed the write"), "{message}");
    }

    /// A process whose volume moved keeps confirming the old leader's
    /// reads when its lease never ends (N21).
    #[test]
    fn negative_control_disk_lease_ends() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::DiskLease { defended: true },
            Controls::DiskLease { defended: false },
            I18,
            "split",
        );
        assert!(message.contains("missed the write"), "{message}");
    }

    /// A successor that crashes during its wait restarts owing nothing
    /// when the start does not record the owed wait (N22).
    #[test]
    fn negative_control_owed_wait_is_recorded() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::OwedWait { defended: true },
            Controls::OwedWait { defended: false },
            I18,
            "split",
        );
        assert!(message.contains("missed the write"), "{message}");
    }

    /// A gate whose barrier runs during the wait writes its floor from a
    /// round its predecessor may still take part in (N13).
    #[test]
    fn negative_control_gate_after_wait() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::GateAfterWait { defended: true },
            Controls::GateAfterWait { defended: false },
            I16,
            "remount",
        );
        assert!(message.contains("while held"), "{message}");
    }

    /// A moved volume read as a process crash under `never` votes with
    /// what it lost, and the third node's leadership replaces acknowledged
    /// writes (N20).
    #[test]
    fn negative_control_remounted_run() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::RemountedRun { defended: true },
            Controls::RemountedRun { defended: false },
            I1,
            "lost_acknowledgements_reads",
        );
        assert!(message.contains("acknowledged"), "{message}");
    }

    /// A leader that counts answers unchecked keeps counting the old
    /// process of a replaced node after the new root made promises (N10).
    #[test]
    fn negative_control_responder_binding() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ResponderBinding { defended: true },
            Controls::ResponderBinding { defended: false },
            I17,
            "release_broken",
        );
        assert!(message.contains("made a promise"), "{message}");
    }

    /// A new disk refused for longer than a wait, whose wait runs from the
    /// start of its run, takes part at once when it binds (N23).
    #[test]
    fn negative_control_wait_from_binding() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::WaitFromBinding { defended: true },
            Controls::WaitFromBinding { defended: false },
            I16,
            "new_disk",
        );
        assert!(
            message.contains("within the wait after its binding"),
            "{message}"
        );
    }
}
