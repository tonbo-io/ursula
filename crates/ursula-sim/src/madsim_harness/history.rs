//! History identity under faults (#581 section 10): a restore through the
//! runtime's import entry point (the one the import route calls; the route
//! itself spawns on a tokio task the simulation does not host) is the
//! restored group's one rewind, and no
//! writer of the source's history ever applies after it. Producers and
//! clients speak HTTP to a runtime that hosts one replica of a three-node
//! OpenRaft group; every answer is parsed with `ursula-headers`.
//!
//! Invariant `rewind_applies_exactly_once` (scenario
//! `history-rewind-under-leader-churn`): the target group's first leader
//! may die before its genesis applied, and the leader dies while the import
//! is in flight or right after it answered. The client repeats the import
//! with the backup's digest until it gets an answer. Every answer names one
//! history; the group's state and every follower hold it with the import's
//! key; HEAD serves the restored stream under the token of that history and
//! names the source's token with the restored tail; the bytes are the
//! backup's; and later leader changes change no token.
//!
//! Invariant `stale_writers_never_apply_after_restore` (scenario
//! `stale-writers-after-restore`): producers keep writing to the source
//! after the backup, and then send those writes to the restored group, with
//! the source's token, without a token, and with the restored token, across
//! leader kills. The source's token answers `412` naming the restored one;
//! every write above the seq that survived answers `403
//! epoch-closed;by=history` with that seq; the surviving newest write
//! answers its receipt; a claim of the next epoch applies and reports the
//! closure. The one write the design lets through (G2 boundary) is a claim
//! by a producer whose record did not survive. The stream ends as the
//! backup's bytes followed by exactly the writes that applied after the
//! restore.
//!
//! [`Controls`] turn off one server defence each, through
//! [`ursula_stream::sim_controls`], for the tests that show the invariants
//! catch its loss.

use axum::Router;
use axum::body::Bytes;
use axum::http::Response;
use ursula_headers::ClosedBy;
use ursula_headers::ClosedEpoch;
use ursula_headers::Delivery;
use ursula_headers::IncarnationMismatch;
use ursula_headers::Inherited;
use ursula_headers::ProducerAnswer;
use ursula_headers::StreamHead;
use ursula_headers::value::Epoch;
use ursula_headers::value::Incarnation;
use ursula_headers::value::Offset;
use ursula_headers::value::Seq;
use ursula_proto::admin::BackupImportResponse;

use super::Body;
use super::Duration;
use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::StatusCode;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::http::body_bytes;
use super::http::send;
use super::write_keys::Cluster;

pub(super) const REWIND_INVARIANT: &str = "rewind_applies_exactly_once";
pub(super) const STALE_INVARIANT: &str = "stale_writers_never_apply_after_restore";
const CONTENT_TYPE: &str = "application/octet-stream";
/// Every body is this long, so the stream splits back into bodies.
const BODY_LEN: usize = 16;
/// Attempts of one request before the client gives up.
const MAX_ATTEMPTS: usize = 60;

/// Server defences a negative control turns off. Production runs use the
/// default, with every defence on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// Row 11 off: a producer record from the source's history keeps its
    /// epoch open after the restore.
    history_fence_off: bool,
    /// G1 off: a write that names the source's token applies to the
    /// restored incarnation.
    incarnation_check_off: bool,
    /// The import is not keyed by its backup: a repeat finds the group
    /// holding state and is refused.
    import_key_off: bool,
}

#[cfg(test)]
impl Controls {
    pub(super) const HISTORY_FENCE_OFF: Self = Self {
        history_fence_off: true,
        incarnation_check_off: false,
        import_key_off: false,
    };
    pub(super) const INCARNATION_CHECK_OFF: Self = Self {
        history_fence_off: false,
        incarnation_check_off: true,
        import_key_off: false,
    };
    pub(super) const IMPORT_KEY_OFF: Self = Self {
        history_fence_off: false,
        incarnation_check_off: false,
        import_key_off: true,
    };
}

/// The defences [`Controls`] turned off for this simulation, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_defences(controls);
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::default());
    }
}

fn set_defences(controls: Controls) {
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::set_off;
    set_off(Defence::HistoryFence, controls.history_fence_off);
    set_off(Defence::IncarnationCheck, controls.incarnation_check_off);
    set_off(Defence::ImportKey, controls.import_key_off);
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// The body of producer `producer`'s write `(epoch, seq)`.
fn body(producer: u8, epoch: u64, seq: u64) -> Vec<u8> {
    let mut body = format!("p{producer}e{epoch}s{seq:03}").into_bytes();
    body.resize(BODY_LEN, b'.');
    body
}

fn pairs<B>(response: &Response<B>) -> impl Iterator<Item = (&str, &[u8])> {
    response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_bytes()))
}

/// What came back for one producer write.
enum Answer {
    Producer(ProducerAnswer),
    Mismatch(IncarnationMismatch),
    /// No typed answer: the write's outcome is unknown.
    Unknown(StatusCode),
}

/// One producer write of `producer`, carrying `token` as its
/// `Stream-Incarnation` when given, sent until it gets a typed answer.
async fn write(
    app: &Router,
    path: &str,
    producer: u8,
    epoch: u64,
    seq: u64,
    token: Option<Incarnation>,
) -> Answer {
    let payload = body(producer, epoch, seq);
    let id = format!("writer-{producer}");
    let epoch_text = epoch.to_string();
    let seq_text = seq.to_string();
    let digest = blake3::hash(&payload).to_hex().to_string();
    let token = token.map(|token| token.to_string());
    let mut headers = vec![
        ("content-type", CONTENT_TYPE),
        ("producer-id", id.as_str()),
        ("producer-epoch", epoch_text.as_str()),
        ("producer-seq", seq_text.as_str()),
        ("producer-digest", digest.as_str()),
    ];
    if let Some(token) = token.as_deref() {
        headers.push(("stream-incarnation", token));
    }
    let mut last = StatusCode::INTERNAL_SERVER_ERROR;
    for _ in 0..MAX_ATTEMPTS {
        let response = send(app, "POST", path, &headers, Body::from(payload.clone())).await;
        let status = response.status().as_u16();
        if let Ok(Some(mismatch)) = IncarnationMismatch::parse(status, pairs(&response)) {
            return Answer::Mismatch(mismatch);
        }
        if let Ok(Some(answer)) = ProducerAnswer::parse(status, pairs(&response)) {
            return Answer::Producer(answer);
        }
        last = response.status();
        madsim::time::sleep(Duration::from_millis(20)).await;
    }
    Answer::Unknown(last)
}

/// The stream's HEAD, through the leader.
async fn head(app: &Router, path: &str) -> StreamHead {
    for _ in 0..MAX_ATTEMPTS {
        let response = send(app, "HEAD", path, &[], Body::empty()).await;
        if response.status() == StatusCode::OK {
            return StreamHead::parse(pairs(&response)).expect("a well-formed stream head");
        }
        madsim::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("HEAD {path} never answered");
}

/// Exports the group of `cluster`'s stream.
pub(super) async fn export(cluster: &Cluster) -> Bytes {
    let path = format!(
        "/__ursula/backup/group/{}",
        cluster.placement.raft_group_id.0
    );
    let response = send(&cluster.app, "GET", &path, &[], Body::empty()).await;
    assert_eq!(response.status(), StatusCode::OK, "export the source group");
    body_bytes(response).await
}

/// What one import attempt came back with.
pub(super) enum ImportAnswer {
    Answered(BackupImportResponse),
    /// The group refused the import as a conflict: it holds state, and not
    /// state restored from this backup.
    Conflict,
    /// The outcome is unknown.
    Unknown,
}

/// One import of `backup` into the group of `runtime`, as the import route
/// proposes it: the backup decoded, its digest as the key, and a history id
/// the request draws. (The route itself runs admin mutations on a tokio
/// task, which the simulation does not host.)
async fn import_once(
    runtime: &ursula_runtime::ShardRuntime,
    raft_group_id: ursula_shard::RaftGroupId,
    backup: &Bytes,
) -> ImportAnswer {
    let snapshot = ursula_stream::decode_backup_snapshot(backup).expect("decode the backup");
    let request = ursula_runtime::ImportGroupStateRequest {
        snapshot: Box::new(snapshot),
        history: ursula_runtime::draw_history_id(),
        backup_digest: *blake3::hash(backup).as_bytes(),
        now_ms: 0,
    };
    match runtime.import_group_state(raft_group_id, request).await {
        Ok(response) => ImportAnswer::Answered(BackupImportResponse {
            buckets: response.buckets,
            streams: response.streams,
            group_commit_index: response.group_commit_index,
            history: response.history,
            already: response.already,
        }),
        Err(error)
            if error.stream_error_code()
                == Some(ursula_runtime::StreamErrorCode::ImportConflict) =>
        {
            ImportAnswer::Conflict
        }
        Err(_unknown) => ImportAnswer::Unknown,
    }
}

/// Repeats the import until it gets an answer, a refusal included.
pub(super) async fn import(cluster: &Cluster, backup: &Bytes) -> ImportAnswer {
    for _ in 0..MAX_ATTEMPTS {
        match import_once(&cluster.runtime, cluster.placement.raft_group_id, backup).await {
            ImportAnswer::Unknown => madsim::time::sleep(Duration::from_millis(20)).await,
            answer => return answer,
        }
    }
    panic!("the import never answered");
}

/// The rewind invariant's reading of an import's answer: a client that
/// repeats its own import of an empty group always gets the import's answer.
fn import_answered(trace: &mut SimTrace, answer: ImportAnswer) -> BackupImportResponse {
    match answer {
        ImportAnswer::Answered(answer) => answer,
        ImportAnswer::Conflict => fail(
            trace,
            REWIND_INVARIANT,
            "import_repeated",
            "a repeat of the client's own import was refused as a conflict, so the client \
             cannot tell whether its import applied"
                .to_owned(),
        ),
        ImportAnswer::Unknown => unreachable!("import() repeats an unknown outcome"),
    }
}

/// The source cluster: its stream after `writes` writes of each producer,
/// the backup taken then, and the writes it applied after the backup.
struct Source {
    backup: Bytes,
    token: Incarnation,
    tail: u64,
    bytes: Vec<u8>,
}

async fn source(
    config: &ThreeNodeRaftSimConfig,
    trace: &mut SimTrace,
    producers: u8,
    writes: u64,
    after: u64,
    newcomer: Option<u8>,
) -> Source {
    let cluster = Cluster::start(config, trace).await;
    let token = head(&cluster.app, &cluster.path).await.incarnation();
    for seq in 0..writes {
        for producer in 0..producers {
            match write(&cluster.app, &cluster.path, producer, 0, seq, Some(token)).await {
                Answer::Producer(ProducerAnswer::Receipt {
                    delivery: Delivery::Applied,
                    ..
                }) => {}
                _ => panic!("the source applies writer-{producer} seq {seq}"),
            }
        }
    }
    let bytes = cluster.read_all().await;
    let backup = export(&cluster).await;
    // The source goes on: these writes are not in the backup.
    for seq in writes..writes.saturating_add(after) {
        for producer in 0..producers {
            if !matches!(
                write(&cluster.app, &cluster.path, producer, 0, seq, Some(token)).await,
                Answer::Producer(ProducerAnswer::Receipt { .. })
            ) {
                panic!("the source applies writer-{producer} seq {seq} after the backup");
            }
        }
    }
    if let Some(newcomer) = newcomer
        && !matches!(
            write(&cluster.app, &cluster.path, newcomer, 0, 0, Some(token)).await,
            Answer::Producer(ProducerAnswer::Receipt { .. })
        )
    {
        panic!("the source applies the newcomer's claim after the backup");
    }
    Source {
        backup,
        token,
        tail: u64::try_from(bytes.len()).expect("offset fits u64"),
        bytes,
    }
}

/// Waits until every follower of `cluster`'s group joined it: it holds the
/// group's membership, so the others can elect a new leader once the
/// leader dies.
pub(super) async fn wait_followers_joined(cluster: &Cluster) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    loop {
        let joined = cluster
            .factory
            .follower_histories()
            .iter()
            .all(|(node_id, _)| {
                cluster
                    .factory
                    .follower_raft_handle(*node_id)
                    .is_some_and(|raft| {
                        openraft::rt::WatchReceiver::borrow_watched(&raft.metrics())
                            .membership_config
                            .voter_ids()
                            .count()
                            > 0
                    })
            });
        if joined {
            return;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "the followers never joined the group"
        );
        madsim::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Kills the target's leader and restarts it as a follower.
async fn churn(cluster: &mut Cluster, trace: &mut SimTrace, phase: &str) {
    let old = cluster.kill_leader(trace, phase).await;
    cluster.restart(trace, old).await;
}

/// The plan of one `rewind_applies_exactly_once` run.
#[derive(Debug)]
struct RewindPlan {
    /// The target's first leader dies before or while it proposes the
    /// group's genesis.
    genesis_kill: bool,
    /// 0: no fault; 1: the leader is cut off with the import in flight and
    /// deposed; 2: the leader dies right after the import answered.
    import_fault: u64,
    /// Leader changes after the import.
    later_kills: u64,
}

impl RewindPlan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6869_7374_6f72_7921);
        Self {
            genesis_kill: rng.next_bounded(2) == 0,
            import_fault: rng.next_bounded(3),
            later_kills: 1_u64.saturating_add(rng.next_bounded(2)),
        }
    }
}

pub(super) async fn run_rewind_applies_exactly_once_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = RewindPlan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let source = source(&config, &mut trace, 2, 3, 0, None).await;
    let mut target = Cluster::start_empty(&config, &mut trace).await;
    wait_followers_joined(&target).await;
    if plan.genesis_kill {
        churn(&mut target, &mut trace, "genesis_leader_killed").await;
    }
    let mut answers = Vec::new();
    match plan.import_fault {
        1 => {
            target.isolate_leader();
            let in_flight = {
                let runtime = target.runtime.clone();
                let raft_group_id = target.placement.raft_group_id;
                let backup = source.backup.clone();
                madsim::task::spawn(async move {
                    match import_once(&runtime, raft_group_id, &backup).await {
                        ImportAnswer::Answered(answer) => Some(answer),
                        ImportAnswer::Conflict | ImportAnswer::Unknown => None,
                    }
                })
            };
            madsim::time::sleep(Duration::from_millis(2)).await;
            let (old, first) = target.depose_isolated_leader(&mut trace, in_flight).await;
            target.restart(&mut trace, old).await;
            answers.extend(first);
        }
        2 => {
            let answer = import(&target, &source.backup).await;
            answers.push(import_answered(&mut trace, answer));
            churn(&mut target, &mut trace, "leader_killed_after_import").await;
        }
        _ => {}
    }
    // The client repeats the import, unchanged, until it has an answer, and
    // once more.
    let answer = import(&target, &source.backup).await;
    answers.push(import_answered(&mut trace, answer));
    let repeat = import(&target, &source.backup).await;
    let repeat = import_answered(&mut trace, repeat);
    if !repeat.already {
        fail(
            &mut trace,
            REWIND_INVARIANT,
            "import_repeated",
            format!("a repeat of an answered import applied again: {repeat:?}"),
        );
    }
    answers.push(repeat);
    let history = answers[0].history;
    if let Some(other) = answers.iter().find(|answer| answer.history != history) {
        fail(
            &mut trace,
            REWIND_INVARIANT,
            "import_answered",
            format!("the import answered two histories: {history} and {other:?}"),
        );
    }
    let applied = answers.iter().filter(|answer| !answer.already).count();
    if applied > 1 {
        fail(
            &mut trace,
            REWIND_INVARIANT,
            "import_answered",
            format!("{applied} answers say the import applied now: {answers:?}"),
        );
    }

    // The group's state and every follower hold that history, with the
    // import's key.
    let digest = *blake3::hash(&source.backup).as_bytes();
    let snapshot = target
        .runtime
        .snapshot_group(target.placement.raft_group_id)
        .await
        .expect("snapshot the restored group");
    let event = snapshot
        .stream_snapshot
        .history
        .expect("a restored history");
    if event.id != history || event.imported != Some(digest) {
        fail(
            &mut trace,
            REWIND_INVARIANT,
            "restored_state",
            format!(
                "the restored group holds {event:?}, not history {history} keyed by the backup"
            ),
        );
    }
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while target
        .factory
        .follower_histories()
        .iter()
        .any(|(_, applied)| *applied != Some(event))
    {
        if madsim::time::Instant::now() >= deadline {
            fail(
                &mut trace,
                REWIND_INVARIANT,
                "followers_applied",
                format!(
                    "followers hold {:?}, not {event:?}",
                    target.factory.follower_histories()
                ),
            );
        }
        madsim::time::sleep(Duration::from_millis(20)).await;
    }

    // HEAD serves the restored stream under the new history's token and
    // traces it to the source.
    let created_at_ms = snapshot.stream_snapshot.streams[0].metadata.created_at_ms;
    let token = Incarnation::try_from(ursula_runtime::incarnation_token(
        created_at_ms,
        history.get(),
    ))
    .expect("a token");
    let restored = head(&target.app, &target.path).await;
    let expected_inherited = Inherited {
        previous_incarnation: source.token,
        offset: Offset::from_u64(source.tail),
    };
    if restored.incarnation() != token
        || restored.incarnation() == source.token
        || restored.inherited() != Some(expected_inherited)
    {
        fail(
            &mut trace,
            REWIND_INVARIANT,
            "restored_head",
            format!("HEAD names {restored:?}, not token {token} inheriting {expected_inherited:?}"),
        );
    }
    if target.read_all().await != source.bytes {
        fail(
            &mut trace,
            REWIND_INVARIANT,
            "restored_read",
            "the restored stream's bytes differ from the backup's".to_owned(),
        );
    }
    // A leader change is no history event.
    for kill in 0..plan.later_kills {
        churn(&mut target, &mut trace, "leader_killed_after_rewind").await;
        let now = head(&target.app, &target.path).await;
        if now != restored {
            fail(
                &mut trace,
                REWIND_INVARIANT,
                "leader_changed",
                format!("leader change {kill} changed HEAD from {restored:?} to {now:?}"),
            );
        }
    }
    trace.push(SimEvent::HistoryRewindVerified {
        stream: config.stream.clone(),
        answers: u64::try_from(answers.len()).expect("small"),
        leader_kills: target.leader_kills,
    });
    outcome(&config, &target, trace)
}

fn outcome(
    config: &ThreeNodeRaftSimConfig,
    cluster: &Cluster,
    trace: SimTrace,
) -> ThreeNodeRaftSimOutcome {
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: cluster
            .factory
            .leader_id(cluster.placement.raft_group_id)
            .unwrap_or_default(),
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

/// The plan of one `stale_writers_never_apply_after_restore` run.
#[derive(Debug)]
struct StalePlan {
    producers: u8,
    /// Writes of each producer in the backup.
    writes: u64,
    /// Writes of each producer the source applied after the backup.
    after: u64,
    /// Producers before whose stale writes the leader dies.
    kills: Vec<u8>,
}

impl StalePlan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x7374_616c_6577_7269);
        let producers = 2_u8.saturating_add(u8::try_from(rng.next_bounded(2)).expect("small"));
        let kills = (0..producers)
            .filter(|_| rng.next_bounded(3) == 0)
            .collect();
        Self {
            producers,
            writes: 2_u64.saturating_add(rng.next_bounded(3)),
            after: 1_u64.saturating_add(rng.next_bounded(3)),
            kills,
        }
    }
}

/// Checks that `answer` is the history fence of `producer`'s epoch 0 closed
/// at `last`.
fn assert_history_closed(trace: &mut SimTrace, producer: u8, seq: u64, last: u64, answer: &Answer) {
    let closed = ClosedEpoch {
        epoch: Epoch::try_from(0).expect("an epoch"),
        seq: Seq::try_from(last).expect("a seq"),
    };
    match answer {
        Answer::Producer(ProducerAnswer::EpochClosed(closure))
            if closure.by() == ClosedBy::History && closure.closed_epoch() == closed => {}
        Answer::Producer(answer) => fail(
            trace,
            STALE_INVARIANT,
            "stale_write",
            format!("writer-{producer} seq {seq} of the source's history got {answer:?}"),
        ),
        Answer::Mismatch(mismatch) => fail(
            trace,
            STALE_INVARIANT,
            "stale_write",
            format!("writer-{producer} seq {seq} got 412 {mismatch:?}"),
        ),
        Answer::Unknown(status) => fail(
            trace,
            STALE_INVARIANT,
            "stale_write",
            format!("writer-{producer} seq {seq} got no typed answer ({status})"),
        ),
    }
}

pub(super) async fn run_stale_writers_never_apply_after_restore_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = StalePlan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let newcomer = plan.producers;
    let source = source(
        &config,
        &mut trace,
        plan.producers,
        plan.writes,
        plan.after,
        Some(newcomer),
    )
    .await;
    let mut target = Cluster::start_empty(&config, &mut trace).await;
    wait_followers_joined(&target).await;
    if !matches!(
        import(&target, &source.backup).await,
        ImportAnswer::Answered(_)
    ) {
        panic!("the restore was refused");
    }
    let restored = head(&target.app, &target.path).await.incarnation();
    let mut appended = source.bytes.clone();
    let mut refused = 0_u64;
    let last = plan.writes.saturating_sub(1);
    for producer in 0..plan.producers {
        if plan.kills.contains(&producer) {
            churn(
                &mut target,
                &mut trace,
                "leader_killed_between_stale_writers",
            )
            .await;
        }
        let (app, path) = (target.app.clone(), target.path.clone());
        for seq in plan.writes..plan.writes.saturating_add(plan.after) {
            // The source's token names a retired history.
            match write(&app, &path, producer, 0, seq, Some(source.token)).await {
                Answer::Mismatch(IncarnationMismatch {
                    current: Some(current),
                }) if current == restored => {}
                Answer::Producer(answer) => fail(
                    &mut trace,
                    STALE_INVARIANT,
                    "source_token",
                    format!("writer-{producer} seq {seq} under the source's token got {answer:?}"),
                ),
                other => fail(
                    &mut trace,
                    STALE_INVARIANT,
                    "source_token",
                    format!(
                        "writer-{producer} seq {seq} under the source's token got {}",
                        match other {
                            Answer::Mismatch(mismatch) => format!("{mismatch:?}"),
                            Answer::Unknown(status) => status.to_string(),
                            Answer::Producer(_) => unreachable!("matched above"),
                        }
                    ),
                ),
            }
            // Without a token, and with the restored one: the epoch is
            // closed at the seq that survived.
            for token in [None, Some(restored)] {
                let answer = write(&app, &path, producer, 0, seq, token).await;
                assert_history_closed(&mut trace, producer, seq, last, &answer);
            }
            refused = refused.saturating_add(3);
        }
        // The newest write that survived applied in the restored history.
        match write(&app, &path, producer, 0, last, None).await {
            Answer::Producer(ProducerAnswer::Receipt {
                delivery: Delivery::Repeat,
                ..
            }) => {}
            _ => fail(
                &mut trace,
                STALE_INVARIANT,
                "surviving_repeat",
                format!("writer-{producer}'s surviving seq {last} got no receipt"),
            ),
        }
        // A claim of the next epoch applies and reports the closure.
        match write(&app, &path, producer, 1, 0, Some(restored)).await {
            Answer::Producer(ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                epoch,
                ..
            }) if epoch.closed_epoch().map(|closed| closed.seq.get()) == Some(last) => {
                appended.extend_from_slice(&body(producer, 1, 0));
            }
            _ => fail(
                &mut trace,
                STALE_INVARIANT,
                "claim",
                format!("writer-{producer}'s claim of epoch 1 did not apply closing seq {last}"),
            ),
        }
    }
    // G2 boundary (#581 section 2.10): the newcomer claimed only after the
    // backup, so its record did not survive, and its untokened claim is a
    // new producer's first write.
    let mut boundary_claims = 0_u64;
    match write(&target.app, &target.path, newcomer, 0, 0, None).await {
        Answer::Producer(ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            ..
        }) => {
            appended.extend_from_slice(&body(newcomer, 0, 0));
            boundary_claims = boundary_claims.saturating_add(1);
        }
        _ => fail(
            &mut trace,
            STALE_INVARIANT,
            "boundary_claim",
            "the newcomer's claim did not apply as the documented boundary says".to_owned(),
        ),
    }
    let bytes = target.read_all().await;
    if bytes != appended {
        let stale = bytes
            .chunks(BODY_LEN)
            .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
            .collect::<Vec<_>>();
        fail(
            &mut trace,
            STALE_INVARIANT,
            "final_read",
            format!("the restored stream holds {stale:?}, not the backup and the applied writes"),
        );
    }
    trace.push(SimEvent::StaleWritersVerified {
        stream: config.stream.clone(),
        refused,
        boundary_claims,
        leader_kills: target.leader_kills,
    });
    outcome(&config, &target, trace)
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::REWIND_INVARIANT;
    use super::STALE_INVARIANT;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    fn config(seed: u64, scenario: SimScenario) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", scenario.slug()))
    }

    /// Runs `scenario` at `seed` with `controls` and asserts `invariant`
    /// failed.
    fn assert_fails(scenario: SimScenario, seed: u64, controls: Controls, invariant: &str) {
        let _guard = sim_test_guard();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_history_with_controls_report(
                config(seed, scenario),
                scenario,
                controls,
            )
        });
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{controls:?} left {scenario:?} passing");
        assert!(
            invariant_failed(&SimTrace::last_recorded(), invariant),
            "{controls:?} did not fail `{invariant}`"
        );
    }

    #[test]
    fn negative_control_without_the_history_fence_a_stale_epoch_applies_after_a_restore() {
        assert_fails(
            SimScenario::StaleWritersAfterRestore,
            380,
            Controls::HISTORY_FENCE_OFF,
            STALE_INVARIANT,
        );
    }

    #[test]
    fn negative_control_without_the_token_check_a_write_of_the_source_history_applies() {
        assert_fails(
            SimScenario::StaleWritersAfterRestore,
            380,
            Controls::INCARNATION_CHECK_OFF,
            STALE_INVARIANT,
        );
    }

    #[test]
    fn negative_control_without_the_import_key_a_repeated_restore_is_refused() {
        assert_fails(
            SimScenario::HistoryRewindUnderLeaderChurn,
            375,
            Controls::IMPORT_KEY_OFF,
            REWIND_INVARIANT,
        );
    }

    #[test]
    fn a_rewind_under_leader_churn_applies_once_and_replays() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::HistoryRewindUnderLeaderChurn;
        for seed in 375..=379 {
            let first = ThreeNodeRaftSim::run_history_report(config(seed, scenario), scenario);
            let second = ThreeNodeRaftSim::run_history_report(config(seed, scenario), scenario);
            assert_eq!(first, second, "seed {seed}");
        }
    }

    #[test]
    fn stale_writers_never_apply_after_a_restore_and_replays() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::StaleWritersAfterRestore;
        for seed in 380..=384 {
            let first = ThreeNodeRaftSim::run_history_report(config(seed, scenario), scenario);
            let second = ThreeNodeRaftSim::run_history_report(config(seed, scenario), scenario);
            assert_eq!(first, second, "seed {seed}");
        }
    }
}
