//! Scenario `identity-fence` (#768 addendum, section 7): the process
//! identity fence over a [`MetaCluster`] of three placed nodes whose runs
//! start as servers' do ([`MetaCluster::start_fenced`]): each behind a disk
//! lease of [`LEASE`], as its node of one cluster, its RPCs on both planes
//! stamped with its process's incarnation and gated by its fence.
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
//!   and the third node up.
//! - **lost acknowledgements under never**: the leader and a follower take
//!   appends while the third node is cut off, the follower's volume moves,
//!   and the leader is killed. The third node and the new process meet,
//!   then the leader restarts.
//! - **correlated power loss** under `always`: every node loses its power,
//!   waits out its predecessor and recovers.
//!
//! Its checks:
//!
//! - I16 ([`I16`]): no data replica of a process whose fence holds it out (a
//!   predecessor wait it owes, or a binding its unbound root awaits) grants
//!   a vote, opens its gate or writes a vote floor. A transfer taken shows
//!   as a vote. It reads each replica's gate and vote at every step, so a
//!   vote granted and then given up between two steps goes unseen, and it
//!   reads neither meta replicas nor snapshot objects.
//! - I18 ([`I18`]): no linearizable read invoked after a write was
//!   acknowledged misses that write. It compares each read with its
//!   invocation, so a read invoked before a write is free to miss it.
//! - I1 ([`I1`]): every running replica reads every acknowledged write at
//!   the end.
//! - Liveness ([`RESUME`]): after the faults heal, every group takes a write
//!   within a predecessor wait and a bound.
//!
//! The family checks neither the meta invariants nor the hosting check of
//! `placement-move`: after a correlated power loss every node hosts what its
//! journals hold while no view shows a bootstrap. The negative controls
//! ([`Controls`]) each run one script with one defence off and fail inside
//! it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::lock::Mutex as AsyncMutex;
use openraft::ServerState;
use ursula_config::WalFsync;
use ursula_raft::JournalTuning;
use ursula_raft::RaftGroupEngine;
use ursula_raft::predecessor_wait::PREDECESSOR_WAIT;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;
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
pub(super) const I18: &str = "linearizable_reads_see_acknowledged_writes";
pub(super) const RESUME: &str = "fenced_cluster_writes_resume";

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
}

/// The run's defences, reset when the run ends.
struct Switched;

const SWITCHED: [Defence; 5] = [
    Defence::SuccessorWait,
    Defence::DiskLease,
    Defence::OwedWait,
    Defence::GateAfterWait,
    Defence::RemountedRun,
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

/// I16's and I18's records across a run.
#[derive(Default)]
struct FenceChecks {
    /// How many observations saw a held replica.
    held_observations: u64,
    /// Each acknowledged write, by group: when, and its payload.
    acknowledged: BTreeMap<u32, Vec<(madsim::time::Instant, Vec<u8>)>>,
    /// How many linearizable reads I18 compared.
    reads: u64,
}

impl FenceChecks {
    /// I16 at one step: every data replica of a held process shows the
    /// gate, floor and vote it showed when it opened, before its drivers
    /// ran, or a vote a leader's append gave it. A held process opens its
    /// replicas while held, since a hold begins at the start of its run.
    fn observe(&mut self, cluster: &MetaCluster, trace: &mut SimTrace, after_event: &str) {
        for node_id in cluster.running() {
            let Some(process) = cluster.process(node_id) else {
                continue;
            };
            if !cluster.data.wals[&node_id].raft_wal().fence().holds_data() {
                continue;
            }
            for replica in cluster.hosted_replicas(node_id) {
                let now = ReplicaMarks::of(&replica.raft, &replica.rejoin);
                let opened = replica.opened;
                self.held_observations = self.held_observations.saturating_add(1);
                let broke = if now.gate_open && !opened.gate_open {
                    Some("opened its gate")
                } else if now.floor && !opened.floor {
                    Some("wrote a vote floor")
                } else if now.vote != opened.vote && !now.vote.is_committed() {
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
                            "node {node_id}'s process {process} {broke} in group {} while its \
                             fence held it out: opened {opened:?}, now {now:?}",
                            replica.group
                        ),
                    );
                }
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

    /// Liveness: `group` takes a write through a leader among the running
    /// nodes within a predecessor wait and the bound.
    async fn resume(&mut self, group: u32, phase: &str) {
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + BOUND;
        loop {
            let running = self.cluster.running();
            if let Some(leader) = self.leader(group, &running)
                && self.append_on(group, leader).await
            {
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

    /// The roles of a split of a drawn group, by who leads it now.
    async fn roles(&mut self, phase: &str) -> Roles {
        let group = JOURNAL_GROUPS
            .get(usize::try_from(self.draws.next_bounded(2)).unwrap_or(0))
            .copied()
            .unwrap_or(0);
        let nodes: Vec<u64> = (1..=NODES).collect();
        let deadline = madsim::time::Instant::now() + BOUND;
        let leader = loop {
            if let Some(leader) = self.leader(group, &nodes) {
                break leader;
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
        };
        let others: Vec<u64> = nodes.into_iter().filter(|node| *node != leader).collect();
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

    /// Sleeps until `offset` after the next probe of `node_id`'s current
    /// run, so a volume that moves then leaves its old process `PROBE`
    /// less `offset` until its next probe fails.
    async fn after_probe(&mut self, node_id: u64, offset: Duration) {
        let started = self.cluster.data.wals[&node_id]
            .started_at()
            .expect("the node runs");
        let now = madsim::time::Instant::now();
        let mut tick = started + PROBE;
        while tick <= now {
            tick += PROBE;
        }
        madsim::time::sleep_until(tick + offset).await;
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
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT + BOUND;
        let mut written_at = None;
        loop {
            let now = madsim::time::Instant::now();
            if written_at.is_some_and(|at| now >= at + READ_ON) || now >= deadline {
                break;
            }
            if let Some(far_leader) = self.leader(group, &[far, moved])
                && self.append_on(group, far_leader).await
            {
                written_at.get_or_insert(now);
            }
            madsim::time::sleep(STEP).await;
            self.observe(PHASE);
        }
        reader.abort();
        let reads = reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        self.fence
            .reads_see_writes(&mut self.trace, PHASE, group, &reads);
        self.heal(zombie).await;
        self.resume(group, "split_healed").await;
        self.reads_back("split_reads").await;
    }

    /// Starts `node_id`'s next process, whose RPCs to `cut_from` are cut.
    async fn start_cut_off(&mut self, node_id: u64, cut_from: u64) {
        let process = self.cluster.next_process().expect("a fenced cluster");
        self.cluster.cut_route(process, cut_from);
        self.cluster.start_node(node_id).await;
    }

    /// Ends every partition and route, and kills `zombie`.
    async fn heal(&mut self, zombie: Zombie) {
        self.cluster.clear_routes();
        self.cluster.data.policy.clear();
        zombie.kill().await;
        self.step("healed");
    }

    /// The remount under `never` (T4, D23): a follower's volume moves and
    /// its new process opens `Recovering` beside the old one, whose disk
    /// lease ends at its next probe. The new process writes no floor and
    /// opens no gate during its wait (I16), then rejoins.
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
        self.watch(PHASE, PREDECESSOR_WAIT / 2).await;
        self.heal(zombie).await;
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
        zombie.kill().await;
        self.cluster.stop_node(leader).await;
        self.cluster.data.policy.clear();
        self.cluster.start_node(moved).await;
        self.step(PHASE);
        let deadline = madsim::time::Instant::now() + PREDECESSOR_WAIT / 3;
        while madsim::time::Instant::now() < deadline {
            if let Some(far_leader) = self.leader(group, &[far, moved]) {
                // A write the far side takes now replaces what the killed
                // leader holds.
                self.append_on(group, far_leader).await;
            }
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

    fn finish(mut self) -> ThreeNodeRaftSimOutcome {
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
    }
}

async fn family(seed: u64) -> ThreeNodeRaftSimOutcome {
    let mut draws = SplitMix64::new(seed ^ SALT ^ 1);
    let offset = Duration::from_millis(draws.next_bounded(1000));
    match seed % 5 {
        0 => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.split(offset, false).await;
            run.finish()
        }
        1 => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.split(offset, true).await;
            run.finish()
        }
        2 => {
            let mut run = Run::start(seed, WalFsync::Never).await;
            run.remount().await;
            run.finish()
        }
        3 => {
            let mut run = Run::start(seed, WalFsync::Never).await;
            run.lost_acknowledgements().await;
            run.finish()
        }
        _ => {
            let mut run = Run::start(seed, WalFsync::Always).await;
            run.power_loss().await;
            run.finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::I1;
    use super::I16;
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

    /// The family's seeds hold, the same way twice per seed, and each saw
    /// a held replica.
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
        assert!(message.contains("while its fence held it out"), "{message}");
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
}
