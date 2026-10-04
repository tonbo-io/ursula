//! Leader reads outside keyed streams (AUD §4.7, D10): HTTP
//! `consistency=leader` catch-up reads, HEAD, `/bootstrap` and snapshot reads
//! against a runtime that hosts one replica of a three-node OpenRaft group.
//!
//! Invariant `leader_read_linearizable`: a leader read that answers 200
//! reflects every append acknowledged before it started. While the group is
//! healthy, or only a follower lags, every probe answers 200. A leader cut
//! off from the quorum still believes it leads; its probes must answer 503
//! (leader unknown, retry) instead of a view that misses the writes the new
//! leader acknowledged. Once the new leader serves, the probes see both
//! sides of the failover.

use axum::Router;
use axum::body::Bytes;
use ursula_raft::RaftGroupHandleRegistry;

use super::AppendRequest;
use super::Arc;
use super::AtomicU64;
use super::Body;
use super::ColdWriteAdmission;
use super::HttpState;
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
use super::seeded_follower_id;
use super::sim_network_policy;

const INVARIANT: &str = "leader_read_linearizable";
const SNAPSHOT_BODY: &[u8] = b"leader-read-snapshot";
const CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, Clone, Copy)]
enum Probe {
    CatchUpRead,
    Head,
    Bootstrap,
    Snapshot,
}

impl Probe {
    fn name(self) -> &'static str {
        match self {
            Self::CatchUpRead => "consistency_leader_read",
            Self::Head => "head",
            Self::Bootstrap => "bootstrap",
            Self::Snapshot => "snapshot_read",
        }
    }
}

const PROBES: [Probe; 4] = [
    Probe::CatchUpRead,
    Probe::Head,
    Probe::Bootstrap,
    Probe::Snapshot,
];

/// Seed-derived shape: payload sizes on both sides of the fault, and whether
/// the leader is deposed or a follower only lags.
struct LeaderReadPlan {
    before: Vec<Vec<u8>>,
    after: Vec<Vec<u8>>,
    depose_leader: bool,
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
        let depose_leader = rng.next_bounded(3) != 0;
        Self {
            before,
            after,
            depose_leader,
        }
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
                Probe::CatchUpRead => (acked_offset, Some(acked)),
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
            assert_eq!(
                seen.status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{} on the deposed leader should be a retryable leader-unknown answer",
                probe.name()
            );
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

    if !plan.depose_leader {
        // A lagging follower leaves the leader a quorum: reads stay served.
        let lagging = seeded_follower_id(config.seed, old_leader);
        policy.partition_bidirectional(old_leader, lagging);
        trace.push(SimEvent::FaultApplied {
            phase: "follower_lags".to_owned(),
        });
        for payload in &plan.after {
            append_over_http(&app, &path, payload, &mut acked).await;
            probes
                .serve_acked("follower_lags", &acked, &mut trace)
                .await;
        }
        return ThreeNodeRaftSimOutcome {
            seed: config.seed,
            leader_id: old_leader,
            target_node_id: Some(lagging),
            appended_log_index: 0,
            trace,
        };
    }

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
