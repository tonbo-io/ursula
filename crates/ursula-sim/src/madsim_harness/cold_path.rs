//! Cold-path scenarios extracted from `madsim_harness/mod.rs`
//! (DoD #3 modularity refactor — workloads axis, cold-store-faceted scenarios).

use ursula_runtime::AppendResponse;
use ursula_runtime::ColdFlushCandidate;
use ursula_runtime::FlushColdResponse;

use super::AppendRequest;
use super::Arc;
use super::ColdStore;
use super::ColdStoreFaultEffect;
use super::ColdStoreOperation;
use super::ColdWriteAdmission;
use super::CreateStreamRequest;
use super::DeleteStreamRequest;
use super::Duration;
use super::FlushColdRequest;
use super::GroupEngine;
use super::InMemoryGroupEngineFactory;
use super::InProcessRaftRegistry;
use super::Mutex;
use super::PlanColdFlushRequest;
use super::PlanGroupColdFlushRequest;
use super::RaftGroupEngine;
use super::ReadStreamRequest;
use super::RuntimeConfig;
use super::RuntimeThreading;
use super::ShardRuntime;
use super::SimEvent;
use super::SimTrace;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::assert_cold_live_read_consistency;
use super::build_three_node_cluster_with_cold_store;
use super::duration_ms;
use super::placement;
use super::read_local_payload_eventually;
use super::sim_cold_store;
use super::sim_network_policy;
use super::verify_all_nodes_can_read_payload;
use super::wait_all_nodes_applied;

/// Everything the three-node cold-path scenarios share after bring-up: the
/// fault-injectable cold store, the live cluster handles, and the first
/// planned cold-flush candidate (`b"abcd"` out of the appended `b"abcdef"`).
struct ColdPathSetup {
    trace: SimTrace,
    cold_store: Arc<ColdStore>,
    /// Held so the in-process Raft network outlives the scenario body.
    _registry: InProcessRaftRegistry,
    engines: Vec<RaftGroupEngine>,
    leader_id: u64,
    leader_index: usize,
    appended: AppendResponse,
    candidate: ColdFlushCandidate,
}

/// Shared cold-path prologue: build the three-node cluster with a
/// fault-injectable cold store, create the stream, append `b"abcdef"`, and
/// plan the first cold-flush candidate.
///
/// `trace_append_commit` additionally records the `AppendCommitted` event
/// right after the append (the cold-write-fault scenario asserts on it).
async fn cold_path_setup(
    config: &ThreeNodeRaftSimConfig,
    append_expect: &'static str,
    plan_expect: &'static str,
    trace_append_commit: bool,
) -> ColdPathSetup {
    let mut trace = SimTrace::default();
    let cold_store = Arc::new(sim_cold_store());
    let policy = sim_network_policy();
    let (registry, mut engines, leader_id) =
        build_three_node_cluster_with_cold_store(policy, Some(cold_store.clone())).await;
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::LeaderElected { leader_id });

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");

    engines[leader_index]
        .create_stream(
            CreateStreamRequest::new(config.stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream through simulated leader");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let appended = engines[leader_index]
        .append(
            AppendRequest::from_bytes(config.stream.clone(), b"abcdef".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect(append_expect);
    if trace_append_commit {
        trace.push(SimEvent::AppendCommitted {
            stream: config.stream.clone(),
            log_index: appended.group_commit_index,
        });
    }

    let candidate = engines[leader_index]
        .plan_cold_flush(
            PlanColdFlushRequest {
                stream_id: config.stream.clone(),
                min_hot_bytes: 4,
                max_flush_bytes: 4,
            },
            placement(),
        )
        .await
        .expect(plan_expect)
        .expect("cold flush candidate");
    assert_eq!(candidate.payload, b"abcd");

    ColdPathSetup {
        trace,
        cold_store,
        _registry: registry,
        engines,
        leader_id,
        leader_index,
        appended,
        candidate,
    }
}

/// Shared cold-path publish step: upload the planned chunk (path tagged with
/// `chunk_tag`), publish the cold flush through Raft, and wait until every
/// node applied it.
async fn cold_path_publish(
    setup: &mut ColdPathSetup,
    config: &ThreeNodeRaftSimConfig,
    chunk_tag: &str,
    write_expect: &'static str,
    flush_expect: &'static str,
    applied_description: &'static str,
) -> FlushColdResponse {
    let chunk_path = format!(
        "{}/{}/chunks/seed-{}-{}000000.bin",
        config.stream.bucket_id, config.stream.stream_id, config.seed, chunk_tag
    );
    let object_size = setup
        .cold_store
        .write_chunk(&chunk_path, &setup.candidate.payload)
        .await
        .expect(write_expect);
    setup.trace.push(SimEvent::ColdChunkWritten {
        stream: config.stream.clone(),
        start_offset: setup.candidate.start_offset,
        end_offset: setup.candidate.end_offset,
    });

    let flushed = setup.engines[setup.leader_index]
        .flush_cold(
            FlushColdRequest {
                stream_id: config.stream.clone(),
                chunk: ursula_runtime::ColdChunkRef {
                    start_offset: setup.candidate.start_offset,
                    end_offset: setup.candidate.end_offset,
                    s3_path: chunk_path,
                    object_size,
                },
            },
            placement(),
        )
        .await
        .expect(flush_expect);
    setup.trace.push(SimEvent::ColdFlushed {
        stream: config.stream.clone(),
        hot_start_offset: flushed.hot_start_offset,
        log_index: flushed.group_commit_index,
    });

    wait_all_nodes_applied(
        &setup.engines,
        flushed.group_commit_index,
        applied_description,
    )
    .await;
    flushed
}

pub(super) async fn run_cold_live_read_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_expected_node_id: Option<u64>,
) -> ThreeNodeRaftSimOutcome {
    let mut setup = cold_path_setup(
        &config,
        "append cold/live payload",
        "plan cold flush",
        false,
    )
    .await;
    let flushed = cold_path_publish(
        &mut setup,
        &config,
        "",
        "write cold chunk",
        "publish cold flush",
        "cold flush applied on all nodes",
    )
    .await;

    for (index, engine) in setup.engines.iter_mut().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node index fits u64");
        let mut expected_all = b"abcdef".to_vec();
        if corrupt_expected_node_id == Some(node_id) {
            expected_all.push(b'!');
        }
        let read_all = read_local_payload_eventually(
            engine,
            node_id,
            &config.stream,
            0,
            6,
            b"abcdef",
            "read cold and hot payload",
        )
        .await;
        assert_cold_live_read_consistency(
            node_id,
            &config.stream,
            &read_all.payload,
            &expected_all,
            read_all.next_offset,
            6,
            "cold_live_read_verified",
        );

        read_local_payload_eventually(
            engine,
            node_id,
            &config.stream,
            4,
            2,
            b"ef",
            "read hot suffix payload",
        )
        .await;
        setup.trace.push(SimEvent::ColdLiveReadVerified {
            node_id,
            stream: config.stream.clone(),
        });
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: setup.leader_id,
        target_node_id: None,
        appended_log_index: flushed.group_commit_index,
        trace: setup.trace,
    }
}

pub(super) async fn run_cold_read_fault_inner(
    config: ThreeNodeRaftSimConfig,
    inject_read_fault: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut setup = cold_path_setup(
        &config,
        "append cold/live payload before cold-read fault",
        "plan cold flush",
        false,
    )
    .await;
    let flushed = cold_path_publish(
        &mut setup,
        &config,
        "fault-",
        "write cold chunk before injected read fault",
        "publish cold flush before injected read fault",
        "cold flush applied on all nodes before read fault",
    )
    .await;

    if inject_read_fault {
        let fail_next_read = Arc::new(Mutex::new(true));
        let fail_next_read_policy = Arc::clone(&fail_next_read);
        setup.cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::ReadObjectRange {
                return None;
            }
            let mut should_fail = fail_next_read_policy
                .lock()
                .expect("cold read fault policy mutex");
            if !*should_fail {
                return None;
            }
            *should_fail = false;
            Some(ColdStoreFaultEffect::fail("seeded cold read fault"))
        });
        setup.trace.push(SimEvent::FaultApplied {
            phase: "before_cold_read".to_owned(),
        });
    }

    let faulted_node_id = setup.leader_id;
    let first_read = setup.engines[setup.leader_index]
        .sim_read_local_stream(
            ReadStreamRequest {
                stream_id: config.stream.clone(),
                offset: 0,
                max_len: 6,
                now_ms: 0,
                record: None,
                max_records: None,
            },
            placement(),
        )
        .await;
    if inject_read_fault {
        let fault_message = first_read
            .expect_err("injected cold read fault should fail the first cold read")
            .to_string();
        setup.trace.push(SimEvent::ColdReadFaultObserved {
            node_id: faulted_node_id,
            stream: config.stream.clone(),
            message: fault_message,
        });
    } else {
        let read = first_read.expect("cold read should succeed without read fault");
        assert_eq!(read.payload, b"abcdef");
        assert_eq!(read.next_offset, 6);
    }

    for (index, engine) in setup.engines.iter_mut().enumerate() {
        let read_all = read_local_payload_eventually(
            engine,
            u64::try_from(index + 1).expect("node index fits u64"),
            &config.stream,
            0,
            6,
            b"abcdef",
            "read cold and hot payload after injected cold read fault",
        )
        .await;
        assert_eq!(read_all.next_offset, 6);
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: setup.leader_id,
        target_node_id: Some(faulted_node_id),
        appended_log_index: flushed.group_commit_index,
        trace: setup.trace,
    }
}

pub(super) async fn run_cold_write_fault_inner(
    config: ThreeNodeRaftSimConfig,
    inject_write_fault: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut setup = cold_path_setup(
        &config,
        "append payload before cold-write fault",
        "plan cold flush before write fault",
        true,
    )
    .await;
    let appended_log_index = setup.appended.group_commit_index;

    if inject_write_fault {
        let fail_next_write = Arc::new(Mutex::new(true));
        let fail_next_write_policy = Arc::clone(&fail_next_write);
        setup.cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::WriteChunk {
                return None;
            }
            let mut should_fail = fail_next_write_policy
                .lock()
                .expect("cold write fault policy mutex");
            if !*should_fail {
                return None;
            }
            *should_fail = false;
            Some(ColdStoreFaultEffect::fail("seeded cold write fault"))
        });
        setup.trace.push(SimEvent::FaultApplied {
            phase: "before_cold_write".to_owned(),
        });
    }

    let chunk_path = format!(
        "{}/{}/chunks/seed-{}-write-fault-000000.bin",
        config.stream.bucket_id, config.stream.stream_id, config.seed
    );
    let write_result = setup
        .cold_store
        .write_chunk(&chunk_path, &setup.candidate.payload)
        .await;
    if inject_write_fault {
        let fault_message = write_result
            .expect_err("injected cold write fault should fail the cold upload")
            .to_string();
        setup.trace.push(SimEvent::ColdWriteFaultObserved {
            stream: config.stream.clone(),
            path: chunk_path,
            message: fault_message,
        });
    } else {
        let object_size = write_result.expect("cold write should succeed without write fault");
        assert_eq!(
            object_size,
            u64::try_from(setup.candidate.payload.len()).expect("payload len fits u64")
        );
    }

    wait_all_nodes_applied(
        &setup.engines,
        appended_log_index,
        "append remains applied after failed cold write",
    )
    .await;
    verify_all_nodes_can_read_payload(&mut setup.engines, &config.stream, b"abcdef").await;
    setup
        .trace
        .push(SimEvent::HotReadAfterColdWriteFailureVerified {
            stream: config.stream,
        });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: setup.leader_id,
        target_node_id: None,
        appended_log_index,
        trace: setup.trace,
    }
}

pub(super) async fn run_cold_write_delay_inner(
    config: ThreeNodeRaftSimConfig,
    delay_ms: Option<u64>,
) -> ThreeNodeRaftSimOutcome {
    let mut setup = cold_path_setup(
        &config,
        "append payload before cold-write delay",
        "plan cold flush before write delay",
        false,
    )
    .await;

    let delay = delay_ms.map(Duration::from_millis);
    if let Some(delay) = delay {
        let delay_next_write = Arc::new(Mutex::new(true));
        let delay_next_write_policy = Arc::clone(&delay_next_write);
        setup.cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::WriteChunk {
                return None;
            }
            let mut should_delay = delay_next_write_policy
                .lock()
                .expect("cold write delay policy mutex");
            if !*should_delay {
                return None;
            }
            *should_delay = false;
            Some(ColdStoreFaultEffect::delay(delay))
        });
        setup.trace.push(SimEvent::FaultApplied {
            phase: "before_cold_write".to_owned(),
        });
    }

    let chunk_path = format!(
        "{}/{}/chunks/seed-{}-write-delay-000000.bin",
        config.stream.bucket_id, config.stream.stream_id, config.seed
    );
    let started = madsim::time::Instant::now();
    let object_size = setup
        .cold_store
        .write_chunk(&chunk_path, &setup.candidate.payload)
        .await
        .expect("write cold chunk after injected write delay");
    if let Some(delay) = delay {
        assert!(
            started.elapsed() >= delay,
            "cold write should observe at least the injected virtual delay"
        );
        setup.trace.push(SimEvent::ColdWriteDelayVerified {
            stream: config.stream.clone(),
            delay_ms: duration_ms(delay),
        });
    }
    setup.trace.push(SimEvent::ColdChunkWritten {
        stream: config.stream.clone(),
        start_offset: setup.candidate.start_offset,
        end_offset: setup.candidate.end_offset,
    });

    let flushed = setup.engines[setup.leader_index]
        .flush_cold(
            FlushColdRequest {
                stream_id: config.stream.clone(),
                chunk: ursula_runtime::ColdChunkRef {
                    start_offset: setup.candidate.start_offset,
                    end_offset: setup.candidate.end_offset,
                    s3_path: chunk_path,
                    object_size,
                },
            },
            placement(),
        )
        .await
        .expect("publish cold flush after injected write delay");
    setup.trace.push(SimEvent::ColdFlushed {
        stream: config.stream.clone(),
        hot_start_offset: flushed.hot_start_offset,
        log_index: flushed.group_commit_index,
    });

    wait_all_nodes_applied(
        &setup.engines,
        flushed.group_commit_index,
        "cold flush applied on all nodes after write delay",
    )
    .await;
    verify_all_nodes_can_read_payload(&mut setup.engines, &config.stream, b"abcdef").await;

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: setup.leader_id,
        target_node_id: Some(setup.leader_id),
        appended_log_index: flushed.group_commit_index,
        trace: setup.trace,
    }
}

pub(super) async fn run_cold_delete_fault_inner(
    config: ThreeNodeRaftSimConfig,
    inject_delete_fault: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let cold_store = Arc::new(sim_cold_store());
    let mut runtime_config = RuntimeConfig::new(2, 8);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn hosted runtime with cold store for delete fault");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let placement = runtime.locate(&config.stream);
    runtime
        .create_stream(CreateStreamRequest::new(
            config.stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream before cold delete fault");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let old_payload = b"abcdefghijklmnopqr".to_vec();
    runtime
        .append(AppendRequest::from_bytes(
            config.stream.clone(),
            old_payload.clone(),
        ))
        .await
        .expect("append old stream payload before stale cold candidate");
    let candidates = runtime
        .plan_next_cold_flush_batch(
            placement.raft_group_id,
            PlanGroupColdFlushRequest {
                min_hot_bytes: old_payload.len(),
                max_flush_bytes: old_payload.len(),
            },
            1,
        )
        .await
        .expect("plan stale cold flush candidate before delete fault");
    assert_eq!(candidates.len(), 1);

    runtime
        .delete_stream(DeleteStreamRequest {
            stream_id: config.stream.clone(),
        })
        .await
        .expect("delete old stream before stale cold candidate");
    runtime
        .create_stream(CreateStreamRequest::new(
            config.stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("recreate stream before stale cold candidate");
    let new_payload = b"abcdefghijklmnopq".to_vec();
    runtime
        .append(AppendRequest::from_bytes(
            config.stream.clone(),
            new_payload.clone(),
        ))
        .await
        .expect("append recreated stream before stale cold candidate");

    if inject_delete_fault {
        let fail_next_delete = Arc::new(Mutex::new(true));
        let fail_next_delete_policy = Arc::clone(&fail_next_delete);
        cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::DeleteChunk {
                return None;
            }
            let mut should_fail = fail_next_delete_policy
                .lock()
                .expect("cold delete fault policy mutex");
            if !*should_fail {
                return None;
            }
            *should_fail = false;
            Some(ColdStoreFaultEffect::fail("seeded cold delete fault"))
        });
        trace.push(SimEvent::FaultApplied {
            phase: "before_cold_cleanup".to_owned(),
        });
    }

    let flushed = runtime
        .flush_cold_candidates_batch_for_simulation(candidates)
        .await
        .expect("stale cold candidate should be skipped after uncertain publish");
    assert!(flushed.is_empty());
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, 1);
    assert_eq!(metrics.cold_flush_publishes, 0);
    assert_eq!(metrics.cold_orphan_cleanup_attempts, 0);
    assert_eq!(metrics.cold_orphan_cleanup_errors, 0);

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: config.stream.clone(),
            offset: 0,
            max_len: 32,
            now_ms: 0,
            record: None,
            max_records: None,
        })
        .await
        .expect("read recreated stream after cold cleanup delete fault");
    assert_eq!(read.payload, new_payload);

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: 0,
        trace,
    }
}

pub(super) async fn run_cold_read_delay_inner(
    config: ThreeNodeRaftSimConfig,
    delay_ms: Option<u64>,
) -> ThreeNodeRaftSimOutcome {
    let mut setup = cold_path_setup(
        &config,
        "append cold/live payload before cold-read delay",
        "plan cold flush before read delay",
        false,
    )
    .await;
    let flushed = cold_path_publish(
        &mut setup,
        &config,
        "delay-",
        "write cold chunk before injected read delay",
        "publish cold flush before injected read delay",
        "cold flush applied on all nodes before read delay",
    )
    .await;

    let delay = delay_ms.map(Duration::from_millis);
    if let Some(delay) = delay {
        let delay_next_read = Arc::new(Mutex::new(true));
        let delay_next_read_policy = Arc::clone(&delay_next_read);
        setup.cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::ReadObjectRange {
                return None;
            }
            let mut should_delay = delay_next_read_policy
                .lock()
                .expect("cold read delay policy mutex");
            if !*should_delay {
                return None;
            }
            *should_delay = false;
            Some(ColdStoreFaultEffect::delay(delay))
        });
        setup.trace.push(SimEvent::FaultApplied {
            phase: "before_cold_read".to_owned(),
        });
    }

    let started = madsim::time::Instant::now();
    let read_all = read_local_payload_eventually(
        &setup.engines[setup.leader_index],
        setup.leader_id,
        &config.stream,
        0,
        6,
        b"abcdef",
        "read cold and hot payload after injected cold read delay",
    )
    .await;
    assert_eq!(read_all.next_offset, 6);
    if let Some(delay) = delay {
        assert!(
            started.elapsed() >= delay,
            "cold read should observe at least the injected virtual delay"
        );
        setup.trace.push(SimEvent::ColdReadDelayVerified {
            stream: config.stream.clone(),
            delay_ms: duration_ms(delay),
        });
    }

    for (index, engine) in setup.engines.iter_mut().enumerate() {
        read_local_payload_eventually(
            engine,
            u64::try_from(index + 1).expect("node index fits u64"),
            &config.stream,
            0,
            6,
            b"abcdef",
            "read cold and hot payload after injected cold read delay",
        )
        .await;
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: setup.leader_id,
        target_node_id: Some(setup.leader_id),
        appended_log_index: flushed.group_commit_index,
        trace: setup.trace,
    }
}

pub(super) async fn run_cold_read_truncate_inner(
    config: ThreeNodeRaftSimConfig,
    truncate_returned_len: Option<usize>,
) -> ThreeNodeRaftSimOutcome {
    let mut setup = cold_path_setup(
        &config,
        "append cold/live payload before cold-read truncation",
        "plan cold flush before read truncation",
        false,
    )
    .await;
    let flushed = cold_path_publish(
        &mut setup,
        &config,
        "truncate-",
        "write cold chunk before injected read truncation",
        "publish cold flush before injected read truncation",
        "cold flush applied on all nodes before read truncation",
    )
    .await;

    if let Some(returned_len) = truncate_returned_len {
        let truncate_next_read = Arc::new(Mutex::new(true));
        let truncate_next_read_policy = Arc::clone(&truncate_next_read);
        setup.cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::ReadObjectRange {
                return None;
            }
            let mut should_truncate = truncate_next_read_policy
                .lock()
                .expect("cold read truncation policy mutex");
            if !*should_truncate {
                return None;
            }
            *should_truncate = false;
            Some(ColdStoreFaultEffect::truncate_read_to(returned_len))
        });
        setup.trace.push(SimEvent::FaultApplied {
            phase: "before_cold_read".to_owned(),
        });
    }

    let faulted_node_id = setup.leader_id;
    let first_read = setup.engines[setup.leader_index]
        .sim_read_local_stream(
            ReadStreamRequest {
                stream_id: config.stream.clone(),
                offset: 0,
                max_len: 6,
                now_ms: 0,
                record: None,
                max_records: None,
            },
            placement(),
        )
        .await;
    if let Some(returned_len) = truncate_returned_len {
        let message = first_read
            .expect_err("injected cold read truncation should fail the first cold read")
            .to_string();
        assert!(
            message.contains(&format!("returned {returned_len} bytes")),
            "cold read truncation should surface the short-body length: {message}"
        );
        setup.trace.push(SimEvent::ColdReadTruncateObserved {
            node_id: faulted_node_id,
            stream: config.stream.clone(),
            requested_len: 4,
            returned_len,
            message,
        });
    } else {
        let read = first_read.expect("cold read should succeed without truncation fault");
        assert_eq!(read.payload, b"abcdef");
        assert_eq!(read.next_offset, 6);
    }

    for (index, engine) in setup.engines.iter_mut().enumerate() {
        let read_all = read_local_payload_eventually(
            engine,
            u64::try_from(index + 1).expect("node index fits u64"),
            &config.stream,
            0,
            6,
            b"abcdef",
            "read cold and hot payload after injected cold read truncation",
        )
        .await;
        assert_eq!(read_all.next_offset, 6);
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: setup.leader_id,
        target_node_id: Some(faulted_node_id),
        appended_log_index: flushed.group_commit_index,
        trace: setup.trace,
    }
}
