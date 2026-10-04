//! Leader reads (AUD §4.7, D10): HTTP
//! `consistency=leader` catch-up reads, HEAD, `/bootstrap` and snapshot reads
//! against a runtime that hosts one replica of a three-node OpenRaft group.
//!
//! Invariant `leader_read_linearizable`: a leader read that answers 200
//! reflects every append acknowledged before it started. While the group is
//! healthy every probe answers 200, and so it does across a partition shorter
//! than an election timeout: the leader retries the confirmation until the
//! heal instead of answering 503. That confirmation waits outside the group
//! actor: a `consistency=local` `offset=now` read on the same leader answers
//! before the heal. A leader cut
//! off from the quorum still believes it leads; its probes must answer 503
//! (leader unknown, retry) instead of a view that misses the writes the new
//! leader acknowledged. Once the new leader serves, the probes see both
//! sides of the failover.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::Router;
use axum::body::Bytes;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::LinearizableReadBarrier;
use ursula_runtime::ReadIndexFuture;

use super::AppendRequest;
use super::Arc;
use super::AtomicU64;
use super::Body;
use super::ColdWriteAdmission;
use super::Duration;
use super::HttpState;
use super::MadsimOpenRaftRuntime;
use super::MadsimRuntimeRaftNetworkFactory;
use super::RuntimeConfig;
use super::RuntimeThreading;
use super::ShardRuntime;
use super::SimEvent;
use super::SimHttpWallClock;
use super::SimTrace;
use super::SplitMix64;
use super::StatusCode;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::http::body_bytes;
use super::http::send;
use super::http_offset;
use super::parse_http_offset;
use super::router_with_http_state;
use super::sim_network_policy;

const INVARIANT: &str = "leader_read_linearizable";
const SNAPSHOT_BODY: &[u8] = b"leader-read-snapshot";
const CONTENT_TYPE: &str = "application/octet-stream";

/// Runs a group's ReadIndex barrier in the engine's deterministic openraft
/// scope, as `MadsimScopedGroupEngine` runs engine calls.
pub(super) struct ScopedReadBarrier {
    seed: u64,
    inner: Arc<dyn LinearizableReadBarrier>,
}

impl ScopedReadBarrier {
    pub(super) fn wrap(
        seed: u64,
        inner: Option<Arc<dyn LinearizableReadBarrier>>,
    ) -> Option<Arc<dyn LinearizableReadBarrier>> {
        let inner = inner?;
        Some(Arc::new(Self { seed, inner }))
    }
}

impl LinearizableReadBarrier for ScopedReadBarrier {
    fn confirm(&self) -> ReadIndexFuture {
        Box::pin(MadsimOpenRaftRuntime::scope(
            self.seed,
            self.inner.confirm(),
        ))
    }
}

#[derive(Debug, Clone, Copy)]
enum Probe {
    CatchUpRead,
    Head,
    Bootstrap,
    Snapshot,
    /// A `consistency=local` tail lookup (`offset=now`): no linearizability
    /// promise, so its internal HEAD takes no confirmation either.
    LocalRead,
}

impl Probe {
    fn name(self) -> &'static str {
        match self {
            Self::CatchUpRead => "consistency_leader_read",
            Self::Head => "head",
            Self::Bootstrap => "bootstrap",
            Self::Snapshot => "snapshot_read",
            Self::LocalRead => "consistency_local_read",
        }
    }
}

const PROBES: [Probe; 4] = [
    Probe::CatchUpRead,
    Probe::Head,
    Probe::Bootstrap,
    Probe::Snapshot,
];

/// Seed-derived shape: payload sizes on both sides of the fault.
struct LeaderReadPlan {
    before: Vec<Vec<u8>>,
    after: Vec<Vec<u8>>,
}

impl LeaderReadPlan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6c65_6164_6572_7264);
        let mut payloads = |tag: u8| {
            let count = 1 + rng.next_bounded(3);
            (0..count)
                .map(|index| {
                    let len = usize::try_from(1 + rng.next_bounded(24)).expect("len fits usize");
                    vec![tag + u8::try_from(index).expect("index fits u8"); len]
                })
                .collect::<Vec<_>>()
        };
        let before = payloads(b'a');
        let after = payloads(b'A');
        Self { before, after }
    }
}

struct Observation {
    status: StatusCode,
    next_offset: Option<u64>,
    body: Bytes,
}

/// The probes of one run: where they go and what the snapshot holds.
struct Probes<'a> {
    app: &'a Router,
    path: &'a str,
    snapshot_offset: u64,
}

impl Probes<'_> {
    async fn observe(&self, probe: Probe) -> Observation {
        let path = self.path;
        let uri = match probe {
            Probe::CatchUpRead => format!("{path}?offset=0&consistency=leader"),
            Probe::LocalRead => format!("{path}?offset=now"),
            Probe::Head => path.to_owned(),
            Probe::Bootstrap => format!("{path}/bootstrap"),
            Probe::Snapshot => format!("{path}/snapshot/{}", http_offset(self.snapshot_offset)),
        };
        let method = if matches!(probe, Probe::Head) {
            "HEAD"
        } else {
            "GET"
        };
        let response = send(self.app, method, &uri, &[], Body::empty()).await;
        let status = response.status();
        let next_offset = response
            .headers()
            .get("stream-next-offset")
            .map(parse_http_offset);
        Observation {
            status,
            next_offset,
            body: body_bytes(response).await,
        }
    }

    /// Every probe answers 200 and reflects every acknowledged append.
    async fn serve_acked(&self, phase: &str, acked: &[u8], trace: &mut SimTrace) {
        let acked_offset = u64::try_from(acked.len()).expect("offset fits u64");
        for probe in PROBES {
            let seen = self.observe(probe).await;
            let (want_offset, want_body) = match probe {
                Probe::CatchUpRead | Probe::LocalRead => (acked_offset, Some(acked)),
                Probe::Head | Probe::Bootstrap => (acked_offset, None),
                Probe::Snapshot => (self.snapshot_offset, Some(SNAPSHOT_BODY)),
            };
            if seen.status != StatusCode::OK
                || seen.next_offset != Some(want_offset)
                || want_body.is_some_and(|body| seen.body[..] != *body)
            {
                fail(
                    trace,
                    phase,
                    format!(
                        "{} answered {} at next offset {:?} ({} body bytes); every append \
                         through {want_offset} was acknowledged first",
                        probe.name(),
                        seen.status,
                        seen.next_offset,
                        seen.body.len(),
                    ),
                );
            }
            trace.push(observed(phase, probe, &seen));
        }
    }

    /// A leader cut off from the quorum refuses every probe with a
    /// retryable 503.
    async fn refuse(&self, phase: &str, acked_offset: u64, trace: &mut SimTrace) {
        for probe in PROBES {
            let seen = self.observe(probe).await;
            if seen.status.is_success() {
                fail(
                    trace,
                    phase,
                    format!(
                        "the deposed leader served {} at next offset {:?}, missing the new \
                         leader's appends through {acked_offset}",
                        probe.name(),
                        seen.next_offset
                    ),
                );
            }
            if seen.status != StatusCode::SERVICE_UNAVAILABLE {
                fail(
                    trace,
                    phase,
                    format!(
                        "{} on the deposed leader answered {}, not a retryable leader-unknown \
                         503",
                        probe.name(),
                        seen.status
                    ),
                );
            }
            trace.push(observed(phase, probe, &seen));
        }
    }
}

fn observed(phase: &str, probe: Probe, seen: &Observation) -> SimEvent {
    SimEvent::LeaderReadObserved {
        phase: phase.to_owned(),
        probe: probe.name().to_owned(),
        status: seen.status.as_u16(),
        next_offset: seen.next_offset,
    }
}

fn fail(trace: &mut SimTrace, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: INVARIANT.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{INVARIANT}` failed after `{after_event}`: {message}");
}

async fn append_over_http(app: &Router, path: &str, payload: &[u8], acked: &mut Vec<u8>) {
    let response = send(
        app,
        "POST",
        path,
        &[("content-type", CONTENT_TYPE)],
        Body::from(payload.to_vec()),
    )
    .await;
    assert!(
        response.status().is_success(),
        "append answered {}",
        response.status()
    );
    acked.extend_from_slice(payload);
    let next_offset = response
        .headers()
        .get("stream-next-offset")
        .map(parse_http_offset);
    assert_eq!(
        next_offset,
        Some(u64::try_from(acked.len()).expect("offset fits u64"))
    );
}

pub(super) async fn run_leader_read_linearizability_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let plan = LeaderReadPlan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let factory = MadsimRuntimeRaftNetworkFactory::new(config.seed, policy.clone());
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(runtime_config, factory.clone())
        .expect("spawn hosted runtime over a three-node raft group");
    // A static cluster router, as in production: a forward-to-leader error
    // with no known leader renders as 503 with Retry-After.
    let peers = (1..=3).map(|node_id| (node_id, format!("http://runtime-raft-node-{node_id}")));
    let state = HttpState::with_static_raft_cluster(
        runtime.clone(),
        RaftGroupHandleRegistry::default(),
        peers,
    )
    .with_wall_clock_handle(Arc::new(SimHttpWallClock {
        now_ms: Arc::new(AtomicU64::new(1_000)),
    }));
    let app = router_with_http_state(state);
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let path = format!("/{}/{}", config.stream.bucket_id, config.stream.stream_id);
    let create = send(
        &app,
        "PUT",
        &path,
        &[("content-type", CONTENT_TYPE)],
        Body::empty(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });
    let placement = runtime.locate(&config.stream);
    let old_leader = factory
        .leader_id(placement.raft_group_id)
        .expect("runtime raft leader id");
    trace.push(SimEvent::LeaderElected {
        leader_id: old_leader,
    });

    let mut acked = Vec::new();
    append_over_http(&app, &path, &plan.before[0], &mut acked).await;
    let snapshot_offset = u64::try_from(acked.len()).expect("offset fits u64");
    let publish = send(
        &app,
        "PUT",
        &format!("{path}/snapshot/{}", http_offset(snapshot_offset)),
        &[("content-type", CONTENT_TYPE)],
        Body::from(SNAPSHOT_BODY),
    )
    .await;
    assert_eq!(publish.status(), StatusCode::NO_CONTENT);
    let probes = Probes {
        app: &app,
        path: &path,
        snapshot_offset,
    };
    probes.serve_acked("healthy", &acked, &mut trace).await;
    for payload in &plan.before[1..] {
        append_over_http(&app, &path, payload, &mut acked).await;
        probes.serve_acked("healthy", &acked, &mut trace).await;
    }

    // A partition shorter than an election timeout (50 ms; heartbeats every
    // 10 ms): no election happens, so the leader's first confirmation round
    // fails but a retry after the heal confirms it, and every probe answers
    // 200 instead of 503.
    for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
        policy.partition_bidirectional(old_leader, node_id);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "brief_partition".to_owned(),
    });
    let heal = {
        let policy = policy.clone();
        madsim::task::spawn(async move {
            madsim::time::sleep(Duration::from_millis(15)).await;
            for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
                policy.heal_bidirectional(old_leader, node_id);
            }
        })
    };
    probes
        .serve_acked("brief_partition", &acked, &mut trace)
        .await;
    heal.await.expect("heal the brief partition");

    // The same brief partition stalls a HEAD's confirmation. It waits outside
    // the group actor, so a `consistency=local` tail lookup on this leader answers
    // before the heal, while the HEAD is still waiting; the HEAD answers 200
    // after the heal.
    for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
        policy.partition_bidirectional(old_leader, node_id);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "stalled_confirmation".to_owned(),
    });
    let healed = Arc::new(AtomicBool::new(false));
    let heal = {
        let policy = policy.clone();
        let healed = healed.clone();
        madsim::task::spawn(async move {
            madsim::time::sleep(Duration::from_millis(15)).await;
            for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
                policy.heal_bidirectional(old_leader, node_id);
            }
            healed.store(true, Ordering::SeqCst);
        })
    };
    let head_done = Arc::new(AtomicBool::new(false));
    let stalled_head = {
        let (app, path, head_done) = (app.clone(), path.clone(), head_done.clone());
        madsim::task::spawn(async move {
            let probes = Probes {
                app: &app,
                path: &path,
                snapshot_offset,
            };
            let seen = probes.observe(Probe::Head).await;
            head_done.store(true, Ordering::SeqCst);
            seen
        })
    };
    madsim::time::sleep(Duration::from_millis(1)).await;
    let local = probes.observe(Probe::LocalRead).await;
    let (healed_first, head_first) = (
        healed.load(Ordering::SeqCst),
        head_done.load(Ordering::SeqCst),
    );
    trace.push(observed("stalled_confirmation", Probe::LocalRead, &local));
    let acked_offset = u64::try_from(acked.len()).expect("offset fits u64");
    if local.status != StatusCode::OK
        || local.next_offset != Some(acked_offset)
        || !local.body.is_empty()
        || healed_first
        || head_first
    {
        fail(
            &mut trace,
            "stalled_confirmation",
            format!(
                "the consistency=local offset=now read answered {} at next offset {:?} with \
                 {} body bytes (healed first: {healed_first}, HEAD answered first: \
                 {head_first}); it must answer 200 at {acked_offset} with no body while the \
                 HEAD's confirmation waits for the heal",
                local.status,
                local.next_offset,
                local.body.len(),
            ),
        );
    }
    let head = stalled_head.await.expect("stalled HEAD task");
    trace.push(observed("stalled_confirmation", Probe::Head, &head));
    if head.status != StatusCode::OK || head.next_offset != Some(acked_offset) {
        fail(
            &mut trace,
            "stalled_confirmation",
            format!(
                "the stalled HEAD answered {} at next offset {:?} after the heal; every append \
                 through {acked_offset} was acknowledged first",
                head.status, head.next_offset,
            ),
        );
    }
    heal.await.expect("heal the stalled confirmation");

    // Cut the leader off from both followers. It keeps believing it leads
    // while the majority elects a new leader and acknowledges more appends.
    for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
        policy.partition_bidirectional(old_leader, node_id);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "leader_isolated".to_owned(),
    });
    let (new_leader, mut new_engine) = factory
        .take_current_leader_engine(placement.raft_group_id)
        .await
        .expect("majority elects a new leader");
    trace.push(SimEvent::LeaderElected {
        leader_id: new_leader,
    });
    for payload in &plan.after {
        let response = new_engine
            .append(
                AppendRequest::from_bytes(config.stream.clone(), payload.clone()),
                placement,
                ColdWriteAdmission::default(),
            )
            .await
            .expect("append acknowledged by the new leader");
        acked.extend_from_slice(payload);
        assert_eq!(
            response.next_offset,
            u64::try_from(acked.len()).expect("offset fits u64")
        );
    }
    let acked_offset = u64::try_from(acked.len()).expect("offset fits u64");
    probes
        .refuse("leader_isolated", acked_offset, &mut trace)
        .await;

    runtime
        .shutdown_group_engine_for_simulation(placement)
        .await
        .expect("shut down the deposed leader's engine");
    runtime
        .install_group_engine_for_simulation(placement, new_engine)
        .await
        .expect("install the new leader's engine");
    trace.push(SimEvent::FaultApplied {
        phase: "new_leader_serving".to_owned(),
    });
    probes
        .serve_acked("new_leader_serving", &acked, &mut trace)
        .await;

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: new_leader,
        target_node_id: Some(old_leader),
        appended_log_index: 0,
        trace,
    }
}
