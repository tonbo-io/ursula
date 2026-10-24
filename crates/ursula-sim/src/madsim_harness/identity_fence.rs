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
//! - **two moved volumes** (D24): under `always`, the volumes of a group's
//!   leader and of one follower move together while both old processes run
//!   on, reaching each other and no other process. The old leader answers
//!   linearizable reads while both new processes wait out their
//!   predecessors and then elect a leader with the third node, which takes
//!   writes. Healed, the group takes writes within a bound, with no
//!   operator step.
//! - **a resumed leader** (T3): under `always`, the leader of an idle group
//!   no longer reaches the third node, and its device hangs (its journal
//!   writer holds) while the follower persists one more entry. Then its
//!   volume moves, and its new process starts from what was durable,
//!   without that entry, while the old process dies.
//! - **the zombie leads** (item 20(a) of the round 3 review): under
//!   `always`, a group leader's volume moves while its old process runs on,
//!   and one follower, cut off from the third node and from the new
//!   process, keeps acknowledging the old process. The old process answers
//!   linearizable reads while the new process waits out its predecessor and
//!   then elects a leader with the third node, which takes writes.
//! - **zombie GC** (D13): under `always`, every node runs the production
//!   snapshot driver over the in-memory snapshot store (#687), each process
//!   behind its node fence. A follower snapshots a group twice, so a GC pass
//!   of its own directory is owed, and the pass begins inside its lease,
//!   its LIST held at the store's gate. Each snapshot follows an append the
//!   follower applied, so it builds. Then its volume moves while the old
//!   process runs on, cut off from every node, and the gate holds the LIST
//!   until the new process wrote and named a snapshot object of the group.
//! - **zombie cold jobs** (D29): under `always`, every replica writes one
//!   in-memory cold store, each process's handle behind its node fence. A
//!   group leader's volume moves while its old process runs on, cut off
//!   from every node, so it keeps leading in its server state. The other
//!   two nodes elect a leader, which takes external appends and offloads
//!   their refs from state into the cold index. Once the old process's
//!   lease ended, every node runs the production cold-index repair step,
//!   the old process included, whose stale tail lies below every new
//!   entry.
//!
//! Its checks:
//!
//! - I16 ([`I16`]): no data replica of a process whose fence holds it out (a
//!   predecessor wait it owes, or a binding its unbound root awaits), or
//!   whose unbound root its fence shows bound less than a predecessor wait
//!   ago (`NodeFence::bound_at`), opens its gate, writes a vote floor, or
//!   shows at a step an uncommitted vote other than the one it opened with.
//!   Nor does the meta replica of a process that owes its predecessor
//!   wait, or whose unbound root its fence shows bound less than a wait
//!   ago, open its gate or write a vote floor. It reads each replica at
//!   every step, but not a meta replica's vote, and reads no snapshot
//!   objects. It does not see a granted vote: a candidate that wins commits
//!   the vote with its first heartbeat, well inside a step, and the vote
//!   then reads as a leader's. No control exercises the vote clause. Under
//!   `always` a moved volume's data replicas open with their gates open and
//!   need no floor, so there the data gate and floor clauses judge nothing.
//!   A new disk runs no data replica while held: its meta replica's barrier
//!   rounds are held too (`fence_holds_barrier`), so that replica takes no
//!   meta entries and confirms no view to create one by. In the new-disk
//!   scripts I16 therefore judges the new disk's meta replica alone.
//! - I17 ([`I17`]): no process counts an answer of a process of a node
//!   given after a later process of that node made a promise: granted a
//!   vote or installed a snapshot (an answer a peer counted), or opened the
//!   gate of its meta replica or of a data replica or wrote its vote floor
//!   (seen at a step, and dated from the step before, the earliest it can
//!   have happened). These are
//!   what a predecessor wait holds. An acknowledgement of entries is no
//!   promise: a successor acknowledges during its wait while its
//!   predecessor may still be counted, which the fence allows (addendum
//!   2.1 and 2.6), since under `always` both acknowledge only what they
//!   synced and every leader the successor acknowledges was elected
//!   without the node's vote. It reads every answer a process counted, the
//!   recovery calls' included, and runs at the end of a run.
//! - I18 ([`I18`]): no linearizable read invoked after a write was
//!   acknowledged misses that write. It compares each read with its
//!   invocation, so a read invoked before a write is free to miss it.
//! - I1 ([`I1`]): every running replica reads every acknowledged write at
//!   the end.
//! - I2 ([`I2`]), by process: no two processes of one node lead one group
//!   in one term. It reads each running process's replicas at every step. A
//!   run that did not end cleanly starts as a follower, and no script
//!   restarts a node cleanly, so a later process never leads a term an
//!   earlier one led.
//! - Liveness ([`RESUME`]): after the faults heal, every group takes a write
//!   within a predecessor wait and a bound.
//! - In **zombie cold jobs** I1 reads the external appends through the
//!   cold index pages, a cold read.
//! - In **zombie GC** also the snapshot object checks at every step
//!   (`snapshot_checks`): every key a node's records name exists
//!   (`snapshot_named_keys_exist`), no node deletes outside its directory,
//!   and no restore reads a missing object (`snapshot_reads_found`).
//! - In **replacements** also the meta invariants of `placement-move` (I12
//!   and the hosting checks at every step, I10 at the end), that the node's
//!   healthy process is never refused while a new disk under its id is,
//!   and that the refused root's meta replica runs and holds no meta vote
//!   floor. The meta leader refuses that root's barrier as well as the
//!   root's own fence holding it, so no control isolates the hold.
//!
//! The other scripts check neither the meta invariants nor the hosting
//! check: after a correlated power loss every node hosts what its journals
//! hold while no view shows a bootstrap. The negative controls
//! ([`Controls`]) each run one script with one defence off and fail inside
//! it, but the zombie's two, which each turn off both of their defences.
//! In the zombie leads, the leader predicate refuses every read of a
//! fenced leader before its round, and its sender check stops every round
//! it would start, so either alone keeps its reads current. In the zombie
//! cold jobs, the leader predicate stops the repair step before it reads a
//! page, and the cold store refuses its page PUT, so either alone keeps
//! the cold index whole.

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
use ursula_proto::placement::RootId;
use ursula_proto::placement::RootSlot;
use ursula_raft::JournalTuning;
use ursula_raft::RaftGroupEngine;
use ursula_raft::meta::pause::MetaPausePoint;
use ursula_raft::predecessor_wait::PREDECESSOR_WAIT;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::OffloadColdRefsRequest;
use ursula_runtime::ProposedUpload;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RepairColdIndexRequest;
use ursula_runtime::new_external_payload_path;
use ursula_runtime::node_fence::FenceStatus;
use ursula_runtime::node_fence::fence_now;
use ursula_runtime::snapshot_harness::Gate;
use ursula_runtime::snapshot_harness::GatePass;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::SnapshotStoreGates;
use ursula_runtime::snapshot_harness::StoreOp;
use ursula_shard::RaftGroupId;
use ursula_stream::sim_controls::Defence;
use ursula_stream::sim_controls::fired;
use ursula_stream::sim_controls::reset_fired;
use ursula_stream::sim_controls::set_off;

use super::ColdRepairOutcome;
use super::IDENTITY_FENCE_EXTENDED_SEEDS;
use super::IDENTITY_FENCE_SEEDS;
use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::DriverPlan;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::SnapshotStorePlan;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::meta_checks::BOUND;
use super::meta_checks::MetaChecks;
use super::meta_checks::STEP;
use super::meta_checks::fail;
use super::meta_cluster::MetaCluster;
use super::meta_cluster::Zombie;
use super::meta_cluster::claim;
use super::placed_hosting::I2;
use super::placed_hosting::ReplicaMarks;
use super::snapshot_checks::SnapshotChecks;

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
/// Mixed into the seed of a run's draws.
const SALT: u64 = 0x6964_656e_7469_7479;
/// The snapshot drivers of a zombie GC run: a pass every 100 ms, four
/// entries kept below a snapshot, and no removal due on its age.
const DRIVERS: DriverPlan = DriverPlan {
    max_age: Duration::from_secs(600),
    keep: 4,
    interval: Duration::from_millis(100),
};
/// `raft.install_snapshot_timeout` of every node in a zombie GC run.
const INSTALL_TIMEOUT: Duration = Duration::from_secs(1);

/// The object stores a run's nodes write besides their journals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stores {
    /// The journals only: snapshots stay inline, and no replica has a
    /// cold store.
    Journals,
    /// The production snapshot driver on every node over the in-memory
    /// snapshot store, each process's store behind its node fence.
    Snapshots,
    /// One in-memory cold store, each process's handle behind its node
    /// fence.
    Cold,
}

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
    /// Two moved volumes, with the disk lease on (`true`) or never ending
    /// (N21).
    TwoMoved { defended: bool },
    /// The resumed leader, with a crashed leader demoted (`true`) or
    /// restored as the leader of its term (N16).
    CrashedLeaderDemotion { defended: bool },
    /// The zombie leads, with its sender and arrival lease checks
    /// (`send_lease`) and its leader predicate's fence
    /// (`fenced_leadership`) each on (`true`) or off.
    ZombieLeads {
        send_lease: bool,
        fenced_leadership: bool,
    },
    /// Zombie GC, with a held process's snapshot objects refused (`true`)
    /// or left to the lease (N15).
    WaitCoversObjects { defended: bool },
    /// Zombie GC, with the fence checked before each DELETE (`true`) or
    /// read when the pass's LIST began (N25).
    ObjectWriteCheck { defended: bool },
    /// Zombie cold jobs, with the cold store's check before each PUT and
    /// DELETE (`cold_write_check`) and the leader predicate's fence
    /// (`fenced_leadership`) each on (`true`) or off (N24 turns off both).
    ZombieColdJobs {
        cold_write_check: bool,
        fenced_leadership: bool,
    },
}

/// The run's defences, reset when the run ends.
struct Switched;

const SWITCHED: [Defence; 13] = [
    Defence::SuccessorWait,
    Defence::DiskLease,
    Defence::OwedWait,
    Defence::GateAfterWait,
    Defence::RemountedRun,
    Defence::ResponderBinding,
    Defence::WaitFromBinding,
    Defence::CrashedLeaderDemotion,
    Defence::SendLease,
    Defence::FencedLeadership,
    Defence::WaitCoversObjects,
    Defence::ObjectWriteCheck,
    Defence::ColdWriteCheck,
];

impl Switched {
    /// Turns off the defences `controls` names, each by its own `set_off`
    /// so the DST defences audit sees every switch.
    fn apply(controls: Controls) -> Self {
        set_off(
            Defence::SuccessorWait,
            controls == Controls::SuccessorWait { defended: false },
        );
        set_off(
            Defence::DiskLease,
            controls == Controls::DiskLease { defended: false }
                || controls == Controls::TwoMoved { defended: false },
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
        set_off(
            Defence::CrashedLeaderDemotion,
            controls == Controls::CrashedLeaderDemotion { defended: false },
        );
        set_off(
            Defence::SendLease,
            [true, false].into_iter().any(|fenced_leadership| {
                controls
                    == Controls::ZombieLeads {
                        send_lease: false,
                        fenced_leadership,
                    }
            }),
        );
        set_off(
            Defence::FencedLeadership,
            [true, false].into_iter().any(|other| {
                controls
                    == Controls::ZombieLeads {
                        send_lease: other,
                        fenced_leadership: false,
                    }
                    || controls
                        == Controls::ZombieColdJobs {
                            cold_write_check: other,
                            fenced_leadership: false,
                        }
            }),
        );
        set_off(
            Defence::WaitCoversObjects,
            controls == Controls::WaitCoversObjects { defended: false },
        );
        set_off(
            Defence::ObjectWriteCheck,
            controls == Controls::ObjectWriteCheck { defended: false },
        );
        set_off(
            Defence::ColdWriteCheck,
            [true, false].into_iter().any(|fenced_leadership| {
                controls
                    == Controls::ZombieColdJobs {
                        cold_write_check: false,
                        fenced_leadership,
                    }
            }),
        );
        reset_fired();
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

/// I2's, I16's, I17's and I18's records across a run.
#[derive(Default)]
struct FenceChecks {
    /// How many observations saw a held data replica.
    held_observations: u64,
    /// How many observations saw a held process's meta replica.
    held_meta_observations: u64,
    /// Whether a step saw a held process whose journals hold a data
    /// replica, which it opens while held.
    held_with_journal: bool,
    /// Each acknowledged write, by group: when, and its payload.
    acknowledged: BTreeMap<u32, Vec<(madsim::time::Instant, Vec<u8>)>>,
    /// How many linearizable reads I18 compared.
    reads: u64,
    /// The process that led each group in each term, by node: `(group,
    /// term, node)` to the process's tag.
    leaders: BTreeMap<(u32, u64, u64), u64>,
    /// The first promise of each process, by tag ([`I17`]): from the
    /// answers before `read_answers`, and from the gates and floors the
    /// observations saw.
    promised: BTreeMap<u64, madsim::time::Instant>,
    read_answers: usize,
    /// When the last observation ran.
    observed_at: Option<madsim::time::Instant>,
}

impl FenceChecks {
    /// I16 and I2 at one step. I16: every data replica of a held process
    /// shows the gate and floor it showed when it opened, before its
    /// drivers ran, and either that vote or a committed one. A vote it
    /// granted to a candidate that won is committed by then, so the vote
    /// clause sees only an uncommitted vote at the step. A process is held
    /// while its fence holds it out, and for a predecessor wait after its
    /// fence shows its unbound root bound. Its meta replica shows the gate
    /// and floor it opened with while the process owes its wait, and for a
    /// predecessor wait after that binding, but not while its root awaits
    /// the binding: a forming cluster's meta replicas take their genesis
    /// floors then.
    ///
    /// A held process opens its data replicas while held only when its
    /// journals hold them, since a hold begins at the start of its run. A
    /// new disk creates none until its hold ends: its meta replica's
    /// barrier rounds are held too (`fence_holds_barrier`), so it holds no
    /// floor, takes no meta entries and confirms no view to create a
    /// replica by. Its meta replica is what a new disk's hold keeps out.
    fn observe(&mut self, cluster: &MetaCluster, trace: &mut SimTrace, after_event: &str) {
        let now = fence_now();
        let observed_at = madsim::time::Instant::now();
        let since = self.observed_at.replace(observed_at).unwrap_or(observed_at);
        for node_id in cluster.running() {
            let Some(process) = cluster.process(node_id) else {
                continue;
            };
            self.one_leader_per_term(cluster, trace, after_event, node_id, process);
            self.gate_promises(cluster, node_id, process, since);
            let wal = cluster.data.wals[&node_id].raft_wal();
            let fence = wal.fence();
            let bound_at = fence.bound_at();
            let by_clock = bound_at.is_some_and(|at| now < at + PREDECESSOR_WAIT);
            let by_fence = fence.holds_data();
            if !by_fence && !by_clock {
                continue;
            }
            if !self.held_with_journal
                && wal
                    .local_groups()
                    .is_ok_and(|local| local.iter().any(|(_, log)| log.is_held()))
            {
                self.held_with_journal = true;
            }
            let owes_wait = fence.owes_wait();
            if (owes_wait || by_clock)
                && let Some(meta) = cluster.meta_gate(node_id)
            {
                self.held_meta_observations = self.held_meta_observations.saturating_add(1);
                if let Some(broke) = meta.now.taken_since(meta.opened) {
                    fail(
                        trace,
                        I16,
                        after_event,
                        format!(
                            "node {node_id}'s process {process} {broke} in the meta group while \
                             held (held by its fence: {owes_wait}, within a wait of its binding: \
                             {by_clock}, bound at {bound_at:?}): opened {:?}, now {:?}",
                            meta.opened, meta.now
                        ),
                    );
                }
            }
            for replica in cluster.hosted_replicas(node_id) {
                let now_marks = ReplicaMarks::of(&replica.raft, &replica.rejoin);
                let opened = replica.opened;
                self.held_observations = self.held_observations.saturating_add(1);
                let broke = now_marks.gate.taken_since(opened.gate).or_else(|| {
                    (now_marks.vote != opened.vote && !now_marks.vote.is_committed())
                        .then_some("granted a vote or campaigned")
                });
                if let Some(broke) = broke {
                    fail(
                        trace,
                        I16,
                        after_event,
                        format!(
                            "node {node_id}'s process {process} {broke} in group {} while held \
                             (held by its fence: {by_fence}, within a wait of its binding: \
                             {by_clock}, bound at {bound_at:?}): opened {opened:?}, now \
                             {now_marks:?}",
                            replica.group
                        ),
                    );
                }
            }
        }
    }

    /// I2 at one step, by process: each replica of `node_id`'s current
    /// process `process` that leads leads a term no other process of the
    /// node led.
    fn one_leader_per_term(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
        node_id: u64,
        process: u64,
    ) {
        for replica in cluster.hosted_replicas(node_id) {
            let watch = replica.raft.metrics();
            let (state, term) = {
                let metrics = openraft::rt::WatchReceiver::borrow_watched(&watch);
                (metrics.state, metrics.current_term)
            };
            if state != ServerState::Leader {
                continue;
            }
            let leader = *self
                .leaders
                .entry((replica.group, term, node_id))
                .or_insert(process);
            if leader != process {
                fail(
                    trace,
                    I2,
                    after_event,
                    format!(
                        "node {node_id}'s process {process} leads group {} in term {term}, which \
                         its process {leader} led",
                        replica.group
                    ),
                );
            }
        }
    }

    /// I17's promises at one step: `node_id`'s current process `process`
    /// promised when its meta replica or one of its data replicas opened
    /// its gate or wrote a vote floor since it opened, dated `since`, the
    /// step before, which no gate seen closed or not yet hosted then can
    /// precede.
    fn gate_promises(
        &mut self,
        cluster: &MetaCluster,
        node_id: u64,
        process: u64,
        since: madsim::time::Instant,
    ) {
        if self.promised.get(&process).is_some_and(|at| *at <= since) {
            return;
        }
        let meta = cluster
            .meta_gate(node_id)
            .is_some_and(|meta| meta.now.taken_since(meta.opened).is_some());
        let promised = meta
            || cluster.hosted_replicas(node_id).iter().any(|replica| {
                ReplicaMarks::of(&replica.raft, &replica.rejoin)
                    .gate
                    .taken_since(replica.opened.gate)
                    .is_some()
            });
        if promised {
            let first = self.promised.entry(process).or_insert(since);
            *first = (*first).min(since);
        }
    }

    /// Folds the promises among the answers counted since the last call
    /// into `promised`: a granted vote or an installed snapshot.
    fn answer_promises(&mut self, cluster: &MetaCluster) {
        let started = cluster.started_processes();
        let answers = cluster.data.answers.since(self.read_answers);
        self.read_answers = self.read_answers.saturating_add(answers.len());
        for answer in answers.iter().filter(|answer| answer.promise) {
            if let Some((tag, _)) = process_of(&started, &answer.responder) {
                let first = self.promised.entry(tag).or_insert(answer.at);
                *first = (*first).min(answer.at);
            }
        }
    }

    /// Whether `process` made a promise, reading the answers counted since
    /// the last call.
    fn promised(&mut self, cluster: &MetaCluster, process: u64) -> bool {
        self.answer_promises(cluster);
        self.promised.contains_key(&process)
    }

    /// I17 over every answer a process counted: none came from a process
    /// of a node after a later process of the node had made a promise.
    fn answers_after_promises(
        &mut self,
        cluster: &MetaCluster,
        trace: &mut SimTrace,
        after_event: &str,
    ) {
        self.answer_promises(cluster);
        let started = cluster.started_processes();
        for answer in cluster.data.answers.answers() {
            let Some((tag, node)) = process_of(&started, &answer.responder) else {
                continue;
            };
            let successor = self.promised.iter().find(|(later, at)| {
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
                        "node {:?} counted an answer of node {node}'s process {tag} at {:?}, \
                         after its successor process {later} made a promise at {at:?}",
                        answer.source, answer.at
                    ),
                );
            }
        }
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
    /// The snapshot object checks, when the nodes run the snapshot store.
    snapshots: Option<SnapshotChecks>,
}

impl Run {
    async fn start(seed: u64, fsync: WalFsync) -> Self {
        Self::start_with(seed, fsync, Stores::Journals).await
    }

    /// [`Self::start`], with the object stores `stores` names.
    async fn start_with(seed: u64, fsync: WalFsync, stores: Stores) -> Self {
        let snapshots = stores == Stores::Snapshots;
        super::journal_cluster::set_cold_store(stores == Stores::Cold);
        if snapshots {
            super::journal_cluster::set_snapshot_drivers(Some(DRIVERS));
            super::journal_cluster::set_snapshot_store(Some(SnapshotStorePlan {
                seed: seed ^ SALT,
                faults: SnapshotStoreFaults::none(),
                install_timeout: INSTALL_TIMEOUT,
            }));
        }
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
        // The cluster read them when it was provisioned.
        super::journal_cluster::set_cold_store(false);
        super::journal_cluster::set_snapshot_drivers(None);
        super::journal_cluster::set_snapshot_store(None);
        let mut run = Self {
            cluster,
            trace,
            meta: MetaChecks::default(),
            fence: FenceChecks::default(),
            draws: SplitMix64::new(seed ^ SALT),
            seed,
            appended: BTreeMap::new(),
            meta_checked: false,
            snapshots: snapshots.then(SnapshotChecks::default),
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
        if let Some(snapshots) = self.snapshots.as_mut() {
            snapshots.step(&self.cluster.data, &mut self.trace, phase);
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
        self.judge_reads(PHASE, leader, group, &reads);
        self.heal(vec![zombie]).await;
        self.resume(group, "split_healed").await;
        self.reads_back("split_reads").await;
    }

    /// I18 over the linearizable reads of `group` that `leader`'s old
    /// process completed in `phase`, which fails when it completed none.
    fn judge_reads(&mut self, phase: &str, leader: u64, group: u32, reads: &Arc<Mutex<Vec<Read>>>) {
        let reads = completed(reads);
        if reads.is_empty() {
            fail(
                &mut self.trace,
                I18,
                phase,
                format!(
                    "the old leader {leader} completed no linearizable read of group {group} in \
                     {phase}, so I18 judged nothing"
                ),
            );
        }
        self.fence
            .reads_see_writes(&mut self.trace, phase, group, &reads);
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
        self.fence
            .answers_after_promises(&self.cluster, &mut self.trace, PHASE);
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

    /// A refused root's meta replica holds no vote floor while refused. It
    /// checks the outcome, not one defence: the meta leader refuses the
    /// stranger's barrier for its root (`FenceRefusal::UnboundRoot`), and
    /// the stranger's own fence holds its barrier rounds
    /// (`fence_holds_barrier`), so either one alone keeps this true.
    fn no_meta_floor(&mut self, stranger: &Zombie, node_id: u64, phase: &str) {
        let floor = stranger
            .registry()
            .meta()
            .and_then(|meta| meta.recovery_gate().map(|gate| gate.holds_vote_floor()));
        let message = match floor {
            Some(false) => return,
            Some(true) => format!(
                "node {node_id}'s new disk took a meta vote floor while its registration was {:?}",
                stranger.registration()
            ),
            None => format!(
                "node {node_id}'s new disk shows no meta replica with a recovery gate to read, \
                 registration {:?}",
                stranger.registration()
            ),
        };
        fail(&mut self.trace, REFUSED, phase, message);
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
        self.cluster.start_node(moved).await;
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
        self.cluster.start_node(moved).await;
        self.step("registration_crash_restarted");
        self.watch("registration_crash_restarted", PREDECESSOR_WAIT + PROBE)
            .await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "registration_crash_restarted").await;
        }
        self.reads_back("registration_crash_reads").await;
    }

    /// Makes `leader` lead `group` and take a write through it, within the
    /// bound.
    async fn led_by(&mut self, group: u32, leader: u64, phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            self.lead(group, leader, phase).await;
            if self.append_on(group, leader).await {
                return;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("group {group} took no write through node {leader}"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// The last log index of `node_id`'s current replica of `group`.
    fn last_log_index(&self, node_id: u64, group: u32) -> Option<u64> {
        self.cluster
            .hosted_replicas(node_id)
            .into_iter()
            .find(|replica| replica.group == group)
            .and_then(|replica| {
                openraft::rt::WatchReceiver::borrow_watched(&replica.raft.metrics()).last_log_index
            })
    }

    /// Appends one payload to `group` through `engine` in a task of its
    /// own, whose outcome the run never expects.
    fn spawn_append(
        &mut self,
        engine: Arc<AsyncMutex<RaftGroupEngine>>,
        group: u32,
    ) -> madsim::task::JoinHandle<()> {
        let count = self.appended.entry(group).or_default();
        let payload = format!("g{group}-{count};").into_bytes();
        *count = count.saturating_add(1);
        madsim::task::spawn(async move {
            let appended = engine
                .lock()
                .await
                .append(
                    AppendRequest::from_bytes(group_stream(group), payload),
                    group_placement(group),
                    ColdWriteAdmission::default(),
                )
                .await;
            if let Err(error) = appended {
                tracing::info!(%error, group, "an append the run never expects failed");
            }
        })
    }

    /// Two moved volumes partitioned together (D24): see the module docs.
    /// `offset` is how long after the old leader's last probe both volumes
    /// move.
    async fn two_moved(&mut self, offset: Duration) {
        const PHASE: &str = "two_moved";
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
        self.led_by(group, leader, PHASE).await;
        let engine = self
            .cluster
            .hosted_engine(leader, group)
            .expect("the leader runs the group");
        let leader_process = self.cluster.process(leader).expect("the leader runs");
        let moved_process = self.cluster.process(moved).expect("the follower runs");
        // The reader starts before the move, so it reads while the old
        // leader's lease holds whatever the offset.
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = spawn_reader(engine, leader, group, Arc::clone(&reads));
        self.after_probe(leader, offset).await;
        self.step(PHASE);
        // Each old process keeps reaching the other one, and no other
        // process.
        let old_leader = self.cluster.move_volume(
            leader,
            &format!("identity-fence-{}-moved-{leader}", self.seed),
            &[moved_process],
        );
        let old_follower = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[leader_process],
        );
        for zombie in [&old_leader, &old_follower] {
            self.cluster.cut_route(zombie.process, far);
        }
        self.cluster.start_node(leader).await;
        self.cluster.start_node(moved).await;
        self.step(PHASE);
        // Both new processes wait out their predecessors, then the far side
        // elects and takes writes.
        self.far_side_writes(group, &[far, leader, moved], PHASE)
            .await;
        reader.abort();
        self.judge_reads(PHASE, leader, group, &reads);
        self.heal(vec![old_leader, old_follower]).await;
        self.resume(group, "two_moved_healed").await;
        self.reads_back("two_moved_reads").await;
    }

    /// A resumed leader (T3): see the module docs. The new process starts
    /// as a follower that already voted in its term, so it leads only a
    /// term it wins (I2), and the follower's entry stays a log entry of its
    /// own.
    async fn resumed_leader(&mut self) {
        const PHASE: &str = "resumed_leader";
        let roles = self.roles(PHASE).await;
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("{PHASE}: {roles:?}"),
        });
        let Roles {
            group,
            leader,
            moved: follower,
            far,
        } = roles;
        self.led_by(group, leader, PHASE).await;
        // I2 sees the old process lead its term.
        self.step(PHASE);
        let old_process = self.cluster.process(leader).expect("the leader runs");
        self.cluster.cut_route(old_process, far);
        let store = self.cluster.data.wals[&leader]
            .store(RaftGroupId(group))
            .expect("the leader's replica holds its store");
        let engine = self
            .cluster
            .hosted_engine(leader, group)
            .expect("the leader runs the group");
        let before = self.last_log_index(follower, group);
        // The leader's device hangs: its journal writer holds the next
        // entry, which the follower persists, and nothing commits it.
        store.pause_simulated_writer(true);
        let append = self.spawn_append(engine, group);
        let deadline = madsim::time::Instant::now() + BOUND;
        while self.last_log_index(follower, group) <= before {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    PHASE,
                    format!(
                        "node {follower} never took node {leader}'s held entry of group {group}"
                    ),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        self.step(PHASE);
        // The volume moves with what was durable, and the new process
        // starts on it while the old one still runs.
        let zombie = self.cluster.move_volume(
            leader,
            &format!("identity-fence-{}-moved-{leader}", self.seed),
            &[],
        );
        self.cluster.start_node(leader).await;
        self.step(PHASE);
        // The old process dies with its writer's queue.
        store.abort_simulated_writer();
        append.abort();
        drop(store);
        drop(zombie.kill().await);
        self.watch(PHASE, PROBE * 2).await;
        self.heal(Vec::new()).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "resumed_leader_healed").await;
        }
        self.reads_back("resumed_leader_reads").await;
    }

    /// The zombie leads (item 20(a)): see the module docs. `offset` is how
    /// long after the old leader's last probe its volume moves.
    async fn zombie_leads(&mut self, offset: Duration) {
        const PHASE: &str = "zombie_leads";
        let roles = self.roles(PHASE).await;
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("{PHASE}: {roles:?}"),
        });
        let Roles {
            group,
            leader,
            moved: follower,
            far,
        } = roles;
        self.led_by(group, leader, PHASE).await;
        let engine = self
            .cluster
            .hosted_engine(leader, group)
            .expect("the leader runs the group");
        let follower_process = self.cluster.process(follower).expect("the follower runs");
        // The reader starts before the move, so it reads while the old
        // leader's lease holds whatever the offset.
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = spawn_reader(engine, leader, group, Arc::clone(&reads));
        self.after_probe(leader, offset).await;
        self.step(PHASE);
        // The follower's RPCs to the leader's node keep reaching the old
        // process, which reaches no one but the follower, and the follower
        // never hears of the new term.
        let zombie = self.cluster.move_volume(
            leader,
            &format!("identity-fence-{}-moved-{leader}", self.seed),
            &[follower_process],
        );
        self.cluster.cut_route(zombie.process, far);
        self.cluster
            .data
            .policy
            .partition_bidirectional(follower, far);
        self.start_cut_off(leader, follower).await;
        self.step(PHASE);
        // The new process waits out its predecessor, then elects with the
        // far node, which takes writes.
        self.far_side_writes(group, &[far, leader], PHASE).await;
        reader.abort();
        self.judge_reads(PHASE, leader, group, &reads);
        self.heal(vec![zombie]).await;
        self.resume(group, "zombie_leads_healed").await;
        self.reads_back("zombie_leads_reads").await;
    }

    /// The snapshot object keys `node_id`'s records name now.
    fn named_by(&self, node_id: u64) -> BTreeSet<String> {
        self.cluster
            .data
            .snapshot_names()
            .into_iter()
            .find(|names| names.node_id == node_id)
            .map(|names| names.named)
            .unwrap_or_default()
    }

    /// Whether `node_id`'s current replica of `group` applied an entry
    /// past its published snapshot.
    fn applied_past_snapshot(&self, group: u32, node_id: u64) -> bool {
        self.cluster
            .hosted_replicas(node_id)
            .into_iter()
            .find(|replica| replica.group == group)
            .is_some_and(|replica| {
                let watched = replica.raft.metrics();
                let metrics = openraft::rt::WatchReceiver::borrow_watched(&watched);
                match (
                    metrics.last_applied.map(|id| id.index),
                    metrics.snapshot.map(|id| id.index),
                ) {
                    (Some(applied), Some(snapshot)) => applied > snapshot,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
            })
    }

    /// Appends to `group` through its leader among the running nodes, if
    /// needed, until `node_id`'s replica applied an entry past its
    /// published snapshot, within the bound. An operator's snapshot counts
    /// as covered once the published snapshot reaches the replica's last
    /// applied index, as `POST /snapshot` does, so with nothing applied
    /// past it the request builds nothing. A held replica applies appends
    /// during its wait under `always`.
    async fn apply_past_snapshot(&mut self, group: u32, node_id: u64, phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        let mut acknowledged = false;
        while !self.applied_past_snapshot(group, node_id) {
            if madsim::time::Instant::now() >= deadline {
                let nodes = self.describe(group);
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!(
                        "node {node_id} applied nothing of group {group} past its snapshot: \
                         {nodes}"
                    ),
                );
            }
            if !acknowledged {
                let running = self.cluster.running();
                acknowledged = self.append_through_leader(group, &running).await;
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// How many LISTs of `group`'s objects arrived at `node_id`'s held
    /// `list` gate.
    fn lists_of(gates: &SnapshotStoreGates, list: Gate, node_id: u64, group: u32) -> usize {
        let subject = format!("node-{node_id}/group-{group}");
        gates
            .log()
            .iter()
            .filter(|event| {
                event.gate == list && event.pass == GatePass::Arrived && event.subject == subject
            })
            .count()
    }

    /// An operator's snapshot of `group` on `node_id` within the bound,
    /// after its replica applied an entry past its last snapshot.
    async fn snapshot_on(&mut self, group: u32, node_id: u64, phase: &str) {
        self.apply_past_snapshot(group, node_id, phase).await;
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            match self
                .cluster
                .placed_snapshot(group, node_id, STEP * 20)
                .await
            {
                Ok(_) => break,
                Err(error) if madsim::time::Instant::now() >= deadline => fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("node {node_id} built no snapshot of group {group}: {error}"),
                ),
                Err(_retried) => madsim::time::sleep(STEP).await,
            }
        }
        self.step(phase);
    }

    /// Zombie GC (D13): see the module docs. The new process writes its
    /// first object once its wait ended, by when the old process's lease
    /// had ended too, so the old pass's DELETEs are refused, however early
    /// the pass began.
    async fn zombie_gc(&mut self) {
        const PHASE: &str = "zombie_gc";
        let roles = self.roles(PHASE).await;
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("{PHASE}: {roles:?}"),
        });
        let Roles {
            group,
            leader,
            moved,
            ..
        } = roles;
        self.led_by(group, leader, PHASE).await;
        self.snapshot_on(group, moved, PHASE).await;
        self.led_by(group, leader, PHASE).await;
        // The second snapshot replaces the first, whose key leaves the
        // holds, so a pass is owed at once. It begins now, and its LIST
        // waits at the gate.
        let gates = self.cluster.data.snapshot_gates(moved);
        let list = Gate::before(moved, StoreOp::ListOwn);
        gates.hold(list);
        let arrived = Self::lists_of(&gates, list, moved, group);
        self.snapshot_on(group, moved, PHASE).await;
        let deadline = madsim::time::Instant::now() + BOUND;
        while Self::lists_of(&gates, list, moved, group) <= arrived {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    PHASE,
                    format!("node {moved} began no GC pass of group {group}"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        self.after_probe(moved, SCRIPTED_OFFSET).await;
        self.step(PHASE);
        let zombie = self.cluster.move_volume(
            moved,
            &format!("identity-fence-{}-moved-{moved}", self.seed),
            &[],
        );
        // No one reaches the old process, and it reaches no one, so no
        // peer's answer supersedes it: only its lease and the new
        // process's wait decide its pass's DELETEs.
        for node_id in 1..=NODES {
            self.cluster.cut_route(zombie.process, node_id);
        }
        self.cluster.start_node(moved).await;
        self.step(PHASE);
        // The new process restored what its records named, and snapshots
        // the group as soon as its fence lets it write the object. Each
        // request follows an append it applied, so it builds.
        let restored = self.named_by(moved);
        self.trace.push(SimEvent::SnapshotKeysRestored {
            node_id: moved,
            keys: restored.iter().cloned().collect(),
        });
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + BOUND;
        while self.named_by(moved).is_subset(&restored) {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    PHASE,
                    format!("node {moved}'s new process named no object of its own"),
                );
            }
            self.apply_past_snapshot(group, moved, PHASE).await;
            if let Err(error) = self.cluster.placed_snapshot(group, moved, STEP * 4).await {
                tracing::info!(error, group, moved, "the new process built no snapshot yet");
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        // The old pass's LIST runs now and shows the new object.
        gates.open(list);
        self.watch(PHASE, PROBE * 2).await;
        self.heal(vec![zombie]).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "zombie_gc_healed").await;
        }
        self.reads_back("zombie_gc_reads").await;
    }

    /// Appends one payload to `group` through `node_id`'s replica as an
    /// external append, staged in the cold store first as the front door
    /// stages a large body, and answers whether it was acknowledged.
    async fn append_external_on(&mut self, group: u32, node_id: u64) -> bool {
        let (Some(engine), Some(cold_store)) = (
            self.cluster.hosted_engine(node_id, group),
            self.cluster.data.cold_store(node_id),
        ) else {
            return false;
        };
        let count = self.appended.entry(group).or_default();
        let payload = format!("g{group}-{count};").into_bytes();
        *count = count.saturating_add(1);
        let stream = group_stream(group);
        let s3_path = new_external_payload_path(&stream);
        let object_size = match cold_store.write_chunk(&s3_path, &payload).await {
            Ok(object_size) => object_size,
            Err(error) => {
                tracing::info!(%error, group, node_id, "an external payload was not staged");
                return false;
            }
        };
        let staged = ExternalPayloadRef {
            s3_path,
            payload_len: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            object_size,
        };
        let appended = madsim::time::timeout(APPEND_TIMEOUT, async {
            let mut engine = engine.lock().await;
            let fence = engine.upload_fence()?;
            let request = AppendExternalRequest::from_append_request(
                AppendRequest::from_bytes(stream, Vec::new()),
                staged,
                0,
                ProposedUpload::new(fence),
            );
            Some(
                engine
                    .append_external(request, group_placement(group))
                    .await,
            )
        })
        .await;
        if !matches!(appended, Ok(Some(Ok(_)))) {
            return false;
        }
        self.fence
            .acknowledged
            .entry(group)
            .or_default()
            .push((madsim::time::Instant::now(), payload));
        true
    }

    /// Zombie cold jobs (D29): see the module docs. The old process's
    /// repair step keeps no external page entry at or past its own tail,
    /// and the new leader's offload moved its external refs from state
    /// into those entries, so a page the old process rewrote leaves the
    /// new leader's acknowledged bytes unreadable (I1 by a cold read).
    async fn zombie_cold_jobs(&mut self) {
        const PHASE: &str = "zombie_cold_jobs";
        let roles = self.roles(PHASE).await;
        self.trace.push(SimEvent::FaultApplied {
            phase: format!("{PHASE}: {roles:?}"),
        });
        let Roles {
            group,
            leader,
            moved: follower,
            far,
        } = roles;
        self.led_by(group, leader, PHASE).await;
        let old_engine = self
            .cluster
            .hosted_engine(leader, group)
            .expect("the leader runs the group");
        let old_fence = self.cluster.data.wals[&leader].raft_wal().fence();
        self.after_probe(leader, SCRIPTED_OFFSET).await;
        self.step(PHASE);
        // No one reaches the old process, and it reaches no one, so it
        // leads the group in its server state for as long as it runs.
        let zombie = self.cluster.move_volume(
            leader,
            &format!("identity-fence-{}-moved-{leader}", self.seed),
            &[],
        );
        for node_id in 1..=NODES {
            self.cluster.cut_route(zombie.process, node_id);
        }
        self.cluster.start_node(leader).await;
        self.step(PHASE);
        // The follower and the far node elect a leader, which takes
        // external appends and offloads their refs into the cold index.
        let survivors = [follower, far];
        let deadline = madsim::time::Instant::now() + BOUND;
        let mut externals = 0;
        while externals < EXTERNAL_APPENDS {
            if madsim::time::Instant::now() >= deadline {
                let nodes = self.describe(group);
                fail(
                    &mut self.trace,
                    RESUME,
                    PHASE,
                    format!("group {group} took {externals} external appends: {nodes}"),
                );
            }
            let appended = match self.leader(group, &survivors) {
                Some(new_leader) => self.append_external_on(group, new_leader).await,
                None => false,
            };
            if appended {
                externals = externals.saturating_add(1);
            } else {
                madsim::time::sleep(STEP).await;
                self.observe(PHASE);
            }
        }
        self.offload(group, &survivors, PHASE).await;
        // The fence claims the old process's jobs from the end of its
        // lease, so they run once it ended.
        let deadline = madsim::time::Instant::now() + LEASE + BOUND;
        while old_fence.admits_now() {
            if madsim::time::Instant::now() >= deadline {
                fail(
                    &mut self.trace,
                    RESUME,
                    PHASE,
                    format!("node {leader}'s old process kept its lease"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        // The production repair step on every node, as each node's runtime
        // runs it on a timer, the old process's included: only the leader
        // predicate and the cold store's check stand between it and the
        // pages.
        for node_id in self.cluster.running() {
            if let Some(engine) = self.cluster.hosted_engine(node_id, group) {
                let outcome = repair_step(&engine, group).await;
                tracing::info!(node_id, ?outcome, "a cold-index repair step");
            }
        }
        let outcome = repair_step(&old_engine, group).await;
        drop(old_engine);
        self.trace.push(SimEvent::ZombieColdRepair { outcome });
        self.step(PHASE);
        self.heal(vec![zombie]).await;
        for group in JOURNAL_GROUPS {
            self.resume(group, "zombie_cold_jobs_healed").await;
        }
        self.reads_back("zombie_cold_jobs_reads").await;
    }

    /// The production offload pass on `group`'s leader among `among`,
    /// within the bound: every external ref of its streams moves from state
    /// into the cold index, whatever its age.
    async fn offload(&mut self, group: u32, among: &[u64], phase: &str) {
        let deadline = madsim::time::Instant::now() + BOUND;
        loop {
            let leader = self.leader(group, among);
            let engine = leader.and_then(|leader| self.cluster.hosted_engine(leader, group));
            if let Some(engine) = engine {
                let request = OffloadColdRefsRequest {
                    now_ms: 0,
                    min_age_ms: 0,
                    max_staged_refs: 0,
                    max_streams: JOURNAL_GROUPS.len(),
                };
                let offloaded = madsim::time::timeout(COLD_JOB_TIMEOUT, async {
                    engine
                        .lock()
                        .await
                        .offload_cold_refs(request, group_placement(group))
                        .await
                })
                .await;
                match offloaded {
                    Ok(Ok(report)) if report.refs_offloaded > 0 => {
                        self.step(phase);
                        return;
                    }
                    Ok(Ok(report)) => {
                        tracing::info!(?report, group, "an offload pass moved no ref");
                    }
                    Ok(Err(error)) => {
                        tracing::info!(%error, group, "an offload pass failed");
                    }
                    Err(_elapsed) => tracing::info!(group, "an offload pass timed out"),
                }
            }
            if madsim::time::Instant::now() >= deadline {
                let nodes = self.describe(group);
                fail(
                    &mut self.trace,
                    RESUME,
                    phase,
                    format!("group {group}'s leader offloaded no external ref: {nodes}"),
                );
            }
            madsim::time::sleep(STEP).await;
            self.observe(phase);
        }
    }

    /// Ends the run: I17 over the whole run, and I16 fails when no step
    /// saw a held data replica, so that every run's I16 judged something.
    /// A run whose held processes held no data replica in their journals,
    /// such as new disks, which create none while held, passes with a held
    /// meta replica instead.
    fn finish(mut self) -> ThreeNodeRaftSimOutcome {
        self.fence
            .answers_after_promises(&self.cluster, &mut self.trace, "finished");
        let fence = &self.fence;
        if fence.held_observations == 0 {
            if fence.held_with_journal {
                fail(
                    &mut self.trace,
                    I16,
                    "finished",
                    "a held process's journals held a data replica, and no step saw a held data \
                     replica, so I16 judged none"
                        .to_owned(),
                );
            } else if fence.held_meta_observations == 0 {
                fail(
                    &mut self.trace,
                    I16,
                    "finished",
                    "no step saw a held replica, so I16 judged nothing".to_owned(),
                );
            }
        }
        self.trace.push(SimEvent::IdentityFenceHeld {
            held_observations: self.fence.held_observations,
            linearizable_reads: self.fence.reads,
            defences_fired: SWITCHED
                .iter()
                .fold(0_u64, |sum, defence| sum.saturating_add(fired(*defence))),
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

/// How many external appends the new leader takes in zombie cold jobs.
const EXTERNAL_APPENDS: u32 = 3;
/// How long one cold job of a replica may take.
const COLD_JOB_TIMEOUT: Duration = Duration::from_secs(5);

/// One production cold-index repair step of `engine`'s replica of
/// `group`, a cursor step's from the start, and what it did.
async fn repair_step(engine: &Arc<AsyncMutex<RaftGroupEngine>>, group: u32) -> ColdRepairOutcome {
    let request = RepairColdIndexRequest {
        after: None,
        max_streams: JOURNAL_GROUPS.len(),
        stream: None,
        retention_gc_now_ms: None,
    };
    let repaired = madsim::time::timeout(COLD_JOB_TIMEOUT, async {
        engine
            .lock()
            .await
            .repair_cold_index(request, group_placement(group))
            .await
    })
    .await;
    match repaired {
        Ok(Ok(response)) if response.cycle_completed => ColdRepairOutcome::Repaired {
            pages_rewritten: response.report.pages_rewritten,
        },
        Ok(Ok(_)) => ColdRepairOutcome::NotLeading,
        Ok(Err(error)) => {
            tracing::info!(%error, group, "a cold-index repair step failed");
            ColdRepairOutcome::Failed
        }
        Err(_elapsed) => ColdRepairOutcome::TimedOut,
    }
}

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
        Controls::TwoMoved { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.two_moved(SCRIPTED_OFFSET).await;
            run.finish()
        }
        Controls::CrashedLeaderDemotion { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.resumed_leader().await;
            run.finish()
        }
        Controls::ZombieLeads { .. } => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.zombie_leads(SCRIPTED_OFFSET).await;
            run.finish()
        }
        Controls::WaitCoversObjects { .. } | Controls::ObjectWriteCheck { .. } => {
            let mut run = Run::start_with(seed, WalFsync::Always, Stores::Snapshots).await;
            run.zombie_gc().await;
            run.finish()
        }
        Controls::ZombieColdJobs { .. } => {
            let mut run = Run::start_with(seed, WalFsync::Always, Stores::Cold).await;
            run.zombie_cold_jobs().await;
            run.finish()
        }
    }
}

/// How many scripts the family runs, one per seed in turn.
const SCRIPTS: u64 = 14;

/// The script of `seed`: the family's scripts are numbered over
/// [`IDENTITY_FENCE_SEEDS`] and then [`IDENTITY_FENCE_EXTENDED_SEEDS`],
/// which follows the seeds of every other family.
fn script_of(seed: u64) -> u64 {
    let first = *IDENTITY_FENCE_SEEDS.start();
    let extended = *IDENTITY_FENCE_EXTENDED_SEEDS.start();
    let position = if seed >= extended {
        let before = IDENTITY_FENCE_SEEDS
            .end()
            .wrapping_sub(first)
            .wrapping_add(1);
        seed.wrapping_sub(extended).wrapping_add(before)
    } else {
        seed.wrapping_sub(first)
    };
    position % SCRIPTS
}

/// The family run of `seed`: one script per seed, in turn over the
/// family's seeds ([`script_of`]).
async fn family(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut draws = SplitMix64::new(seed ^ SALT ^ 1);
    let offset = Duration::from_millis(draws.next_bounded(1000));
    let script = script_of(seed);
    let fsync = match script {
        2 | 3 => WalFsync::Never,
        _ => WalFsync::Always,
    };
    let stores = match script {
        12 => Stores::Snapshots,
        13 => Stores::Cold,
        _ => Stores::Journals,
    };
    let mut run = Run::start_with(seed, fsync, stores).await;
    match script {
        0 => run.split(offset, false).await,
        1 => run.split(offset, true).await,
        2 => run.remount().await,
        3 => run.lost_acknowledgements().await,
        4 => run.power_loss().await,
        5 => run.release_broken().await,
        6 => run.new_disk().await,
        7 => run.replacements().await,
        8 => run.registration_crash().await,
        9 => run.two_moved(offset).await,
        10 => run.resumed_leader().await,
        11 => run.zombie_leads(offset).await,
        12 => run.zombie_gc().await,
        _ => run.zombie_cold_jobs().await,
    }
    run.finish()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::Controls;
    use super::I1;
    use super::I2;
    use super::I16;
    use super::I17;
    use super::I18;
    use super::SCRIPTS;
    use super::script_of;
    use crate::madsim_harness::ColdRepairOutcome;
    use crate::madsim_harness::IDENTITY_FENCE_EXTENDED_SEEDS;
    use crate::madsim_harness::IDENTITY_FENCE_SEEDS;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimReport;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::snapshot_checks::NAMED_KEYS_EXIST;
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

    /// The run ended held, with this many held observations, reads and
    /// outcomes its switched-off defences changed.
    fn held(report: &SimReport) -> Option<(u64, u64, u64)> {
        report
            .outcome
            .trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::IdentityFenceHeld {
                    held_observations,
                    linearizable_reads,
                    defences_fired,
                } => Some((*held_observations, *linearizable_reads, *defences_fired)),
                _ => None,
            })
    }

    /// What the old process's repair step did in a zombie cold jobs run.
    fn cold_repair(trace: &SimTrace) -> Option<ColdRepairOutcome> {
        trace.events.iter().find_map(|event| match event {
            SimEvent::ZombieColdRepair { outcome } => Some(*outcome),
            _ => None,
        })
    }

    /// Each seed of the family's two ranges runs one script, and every
    /// script has a seed.
    #[test]
    fn identity_fence_seeds_cover_every_script() {
        let seeds: Vec<u64> = IDENTITY_FENCE_SEEDS
            .chain(IDENTITY_FENCE_EXTENDED_SEEDS)
            .collect();
        assert_eq!(u64::try_from(seeds.len()).ok(), Some(SCRIPTS));
        let scripts: BTreeSet<u64> = seeds.iter().map(|seed| script_of(*seed)).collect();
        assert_eq!(scripts, (0..SCRIPTS).collect::<BTreeSet<u64>>());
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
            let (held_observations, _, _) = held(&first).expect("the run ends held");
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
        assert!(
            message.contains("opened its gate") || message.contains("wrote a vote floor"),
            "{message}"
        );
        assert!(message.contains("held by its fence: true"), "{message}");
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
        assert!(message.contains("held by its fence: false"), "{message}");
        assert!(
            message.contains("within a wait of its binding: true"),
            "{message}"
        );
    }

    /// Two moved volumes whose old processes reach each other keep
    /// confirming the old leader's reads when their leases never end (N21
    /// in D24).
    #[test]
    fn negative_control_two_moved_volumes() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::TwoMoved { defended: true },
            Controls::TwoMoved { defended: false },
            I18,
            "two_moved",
        );
        assert!(message.contains("missed the write"), "{message}");
    }

    /// The new process of a leader whose device hung resumes the
    /// leadership of its term when a crashed leader is not demoted, so two
    /// processes of its node lead one term (N16).
    #[test]
    fn negative_control_crashed_leader_demotion() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::CrashedLeaderDemotion { defended: true },
            Controls::CrashedLeaderDemotion { defended: false },
            I2,
            "resumed_leader",
        );
        assert!(message.contains("which its process"), "{message}");
    }

    /// A zombie leader that neither its sender and arrival lease checks
    /// nor its leader predicate stop keeps confirming reads with a follower
    /// that never heard of the new term (item 20(a)).
    #[test]
    fn negative_control_zombie_leader_lease() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ZombieLeads {
                send_lease: true,
                fenced_leadership: true,
            },
            Controls::ZombieLeads {
                send_lease: false,
                fenced_leadership: false,
            },
            I18,
            "zombie_leads",
        );
        assert!(message.contains("missed the write"), "{message}");
    }

    /// A successor that writes snapshot objects during its wait writes one
    /// while its predecessor's lease holds, and the predecessor's pass,
    /// which listed it, deletes it (N15).
    #[test]
    fn negative_control_wait_covers_objects() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::WaitCoversObjects { defended: true },
            Controls::WaitCoversObjects { defended: false },
            NAMED_KEYS_EXIST,
            "zombie_gc",
        );
        assert_names_a_new_object(&message);
    }

    /// A pass that reads the fence when its LIST began, inside the lease,
    /// deletes the successor's object long after the lease ended (N25).
    #[test]
    fn negative_control_object_write_check() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ObjectWriteCheck { defended: true },
            Controls::ObjectWriteCheck { defended: false },
            NAMED_KEYS_EXIST,
            "zombie_gc",
        );
        assert_names_a_new_object(&message);
    }

    /// The missing key of a zombie GC failure is one the moved node names,
    /// and none it restored: the new process wrote it.
    fn assert_names_a_new_object(message: &str) {
        let (node_id, restored) = SimTrace::last_recorded()
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::SnapshotKeysRestored { node_id, keys } => Some((*node_id, keys.clone())),
                _ => None,
            })
            .expect("the new process restored its records");
        assert!(
            message.starts_with(&format!("I1: node {node_id} names ")),
            "{message}"
        );
        assert!(
            restored.iter().all(|key| !message.contains(key.as_str())),
            "{message} names a restored key"
        );
    }

    /// The old process's repair step, which a fenced leader predicate and a
    /// fenced cold store would both stop, rewrites the new leader's page
    /// once its lease ended, dropping the entries of its offloaded external
    /// appends, so no replica reads those acknowledged bytes (N24, D29).
    #[test]
    fn negative_control_zombie_cold_jobs() {
        let _guard = sim_test_guard();
        let message = assert_caught(
            Controls::ZombieColdJobs {
                cold_write_check: true,
                fenced_leadership: true,
            },
            Controls::ZombieColdJobs {
                cold_write_check: false,
                fenced_leadership: false,
            },
            I1,
            "zombie_cold_jobs_reads",
        );
        assert!(message.contains("acknowledged writes"), "{message}");
        let outcome = cold_repair(&SimTrace::last_recorded());
        assert!(
            matches!(outcome, Some(ColdRepairOutcome::Repaired { pages_rewritten }) if pages_rewritten > 0),
            "{outcome:?}"
        );
    }

    /// Either defence against a zombie's cold jobs alone keeps the cold
    /// index whole. With both on the old process's repair step answers as
    /// a follower's. With the leader predicate's fence off it runs as a
    /// leader's and the cold store refuses its page PUT. With the cold
    /// store's check off the predicate stops it before any PUT.
    #[test]
    fn either_defence_against_zombie_cold_jobs_alone_suffices() {
        let _guard = sim_test_guard();
        for (cold_write_check, fenced_leadership, expected) in [
            (true, true, ColdRepairOutcome::NotLeading),
            (true, false, ColdRepairOutcome::Failed),
            (false, true, ColdRepairOutcome::NotLeading),
        ] {
            let controls = Controls::ZombieColdJobs {
                cold_write_check,
                fenced_leadership,
            };
            let report = run(SCRIPTED, controls);
            assert!(held(&report).is_some(), "{controls:?} passes");
            assert_eq!(
                cold_repair(&report.outcome.trace),
                Some(expected),
                "{controls:?}"
            );
        }
    }

    /// Either defence of a zombie leader alone keeps its reads current, and
    /// each run shows that the defence it turned off changed an outcome.
    /// With the sender and arrival checks off, the zombie keeps sending and
    /// counting, and its leader predicate refuses every read before its
    /// round. With the predicate's fence off, the zombie starts reads, and
    /// its sender check stops every round they wait for.
    #[test]
    fn either_defence_against_a_zombie_leader_alone_suffices() {
        let _guard = sim_test_guard();
        for controls in [
            Controls::ZombieLeads {
                send_lease: false,
                fenced_leadership: true,
            },
            Controls::ZombieLeads {
                send_lease: true,
                fenced_leadership: false,
            },
        ] {
            let report = run(SCRIPTED, controls);
            let (_, reads, fired) = held(&report).unwrap_or_else(|| panic!("{controls:?} passes"));
            assert!(reads > 0, "{controls:?}: I18 compared no read");
            assert!(
                fired > 0,
                "{controls:?}: the defence turned off never changed an outcome"
            );
        }
    }
}
