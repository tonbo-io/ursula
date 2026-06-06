//! Runtime-actor + runtime/Raft scenarios extracted from `madsim_harness/mod.rs`
//! (DoD #3 modularity refactor — workloads axis).

use super::AppendBatchRequest;
use super::AppendRequest;
use super::Arc;
use super::BTreeSet;
use super::ColdStoreFaultEffect;
use super::ColdStoreOperation;
use super::CreateStreamRequest;
use super::Duration;
use super::HeadStreamRequest;
use super::InMemoryGroupEngineFactory;
use super::MadsimRuntimeRaftNetworkFactory;
use super::MadsimScopedRaftGroupEngineFactory;
use super::Mutex;
use super::PlanGroupColdFlushRequest;
use super::ReadStreamRequest;
use super::RuntimeConfig;
use super::RuntimeInterleavingPlan;
use super::RuntimeRaftNetworkOptions;
use super::RuntimeThreading;
use super::ShardRuntime;
use super::SimEvent;
use super::SimTrace;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::assert_cold_live_read_consistency;
use super::assert_runtime_interleaving_read_your_write;
use super::assert_runtime_raft_leader_failover_read_consistency;
use super::assert_runtime_raft_producer_duplicate;
use super::assert_runtime_raft_producer_stale_epoch;
use super::assert_runtime_raft_read_consistency;
use super::choose_runtime_streams_spanning_placement;
use super::duration_ms;
use super::maybe_panic_after_runtime_interleaving_event;
use super::runtime_interleaving_payload;
use super::runtime_raft_network_batch_payloads;
use super::runtime_raft_network_duplicate_payloads;
use super::runtime_raft_network_producer;
use super::runtime_raft_network_producer_with_lane;
use super::runtime_raft_network_streams;
use super::seeded_follower_id;
use super::sim_cold_store;
use super::sim_network_policy;
use super::verify_runtime_raft_close_stream;
use super::verify_runtime_raft_partial_read;
use super::verify_runtime_raft_snapshot_publish;
use super::verify_runtime_raft_tail_read;
use super::wait_raft_applied_index_at_least;

pub(super) async fn run_runtime_actor_scheduling_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(2, 4);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    runtime_config.mailbox_capacity = 4;
    let runtime = ShardRuntime::spawn(runtime_config).expect("spawn hosted runtime actors");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::RuntimeActorsBuilt {
        core_count: 2,
        raft_group_count: 4,
    });

    runtime
        .create_stream(CreateStreamRequest::new(
            config.stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream through hosted runtime actors");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let wait_runtime = runtime.clone();
    let wait_stream = config.stream.clone();
    let wait_task = madsim::task::spawn(async move {
        wait_runtime
            .wait_read_stream(ReadStreamRequest {
                stream_id: wait_stream,
                offset: 0,
                max_len: 3,
                now_ms: 0,
            })
            .await
    });
    trace.push(SimEvent::RuntimeWaitReadStarted {
        stream: config.stream.clone(),
        offset: 0,
        max_len: 3,
    });

    let delay = Duration::from_millis(50);
    madsim::time::sleep(delay).await;
    trace.push(SimEvent::RuntimeAppendAfterDelay {
        stream: config.stream.clone(),
        delay_ms: duration_ms(delay),
    });

    let append = runtime
        .append(AppendRequest::from_bytes(
            config.stream.clone(),
            b"abc".to_vec(),
        ))
        .await
        .expect("append through hosted runtime actors");
    trace.push(SimEvent::RuntimeAppendCommitted {
        stream: config.stream.clone(),
        start_offset: append.start_offset,
        next_offset: append.next_offset,
    });

    let waited = wait_task
        .await
        .expect("join wait_read runtime task")
        .expect("wait_read completes after append");
    assert_eq!(waited.payload, b"abc");
    assert_eq!(waited.next_offset, 3);
    trace.push(SimEvent::RuntimeWaitReadSatisfied {
        stream: config.stream.clone(),
        payload_len: waited.payload.len(),
    });

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: config.stream.clone(),
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read through hosted runtime actors");
    assert_eq!(read.payload, b"abc");
    trace.push(SimEvent::RuntimeReadVerified {
        stream: config.stream,
        next_offset: read.next_offset,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: append.group_commit_index,
        trace,
    }
}

pub(super) async fn run_runtime_raft_engine_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(
        runtime_config,
        MadsimScopedRaftGroupEngineFactory::new(config.seed),
    )
    .expect("spawn hosted runtime with raft group engine factory");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::RuntimeRaftEngineBuilt {
        core_count: 1,
        raft_group_count: 1,
        raft_node_count: 1,
    });

    runtime
        .create_stream(CreateStreamRequest::new(
            config.stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream through runtime-owned raft engine");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let append_batch = runtime
        .append_batch(AppendBatchRequest::new(config.stream.clone(), vec![
            b"raft-".to_vec(),
            b"runtime".to_vec(),
        ]))
        .await
        .expect("append through runtime-owned raft engine");
    let append_items = append_batch
        .items
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("runtime raft append batch items");
    let append = append_items
        .last()
        .cloned()
        .expect("runtime raft append batch response");
    trace.push(SimEvent::RuntimeRaftEngineAppendCommitted {
        stream: config.stream.clone(),
        start_offset: append_items
            .first()
            .expect("runtime raft first append batch response")
            .start_offset,
        next_offset: append.next_offset,
        group_commit_index: append.group_commit_index,
    });

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id: config.stream.clone(),
            offset: 0,
            max_len: 64,
            now_ms: 0,
        })
        .await
        .expect("read through runtime-owned raft engine");
    assert_eq!(read.payload, b"raft-runtime");
    assert_eq!(read.next_offset, append.next_offset);

    let metrics = runtime.metrics().snapshot();
    assert!(
        metrics.raft_write_many_batches > 0,
        "runtime raft engine scenario should submit writes through OpenRaft"
    );
    assert!(
        metrics.raft_apply_entries > 0,
        "runtime raft engine scenario should apply OpenRaft entries"
    );
    trace.push(SimEvent::RuntimeRaftEngineReadVerified {
        stream: config.stream,
        next_offset: read.next_offset,
        raft_write_many_batches: metrics.raft_write_many_batches,
        raft_apply_entries: metrics.raft_apply_entries,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 1,
        target_node_id: None,
        appended_log_index: append.group_commit_index,
        trace,
    }
}

pub(super) async fn run_runtime_raft_snapshot_install_inner(
    config: ThreeNodeRaftSimConfig,
    corrupt_append_counts: bool,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let source_factory = MadsimRuntimeRaftNetworkFactory::new(config.seed, policy);
    let mut source_config = RuntimeConfig::new(1, 1);
    source_config.threading = RuntimeThreading::HostedTokio;
    let source_runtime =
        ShardRuntime::spawn_with_engine_factory(source_config, source_factory.clone())
            .expect("spawn hosted source runtime with multi-node raft group");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    source_runtime
        .create_stream(CreateStreamRequest::new(
            config.stream.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream before runtime raft snapshot");
    trace.push(SimEvent::StreamCreated {
        stream: config.stream.clone(),
    });

    let placement = source_runtime.locate(&config.stream);
    let source_leader_id = source_factory
        .leader_id(placement.raft_group_id)
        .expect("source runtime raft network leader id");
    trace.push(SimEvent::RuntimeRaftNetworkBuilt {
        core_count: 1,
        raft_group_count: 1,
        raft_node_count: 3,
        leader_id: source_leader_id,
    });

    let append_batch = source_runtime
        .append_batch(AppendBatchRequest::new(config.stream.clone(), vec![
            b"snapshot-".to_vec(),
            b"runtime".to_vec(),
        ]))
        .await
        .expect("append before runtime raft snapshot");
    let append_items = append_batch
        .items
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("runtime raft snapshot source append batch items");
    let append = append_items
        .last()
        .cloned()
        .expect("runtime raft snapshot source append response");
    trace.push(SimEvent::RuntimeRaftEngineAppendCommitted {
        stream: config.stream.clone(),
        start_offset: append_items
            .first()
            .expect("runtime raft snapshot source first append response")
            .start_offset,
        next_offset: append.next_offset,
        group_commit_index: append.group_commit_index,
    });

    let source_read = source_runtime
        .read_stream(ReadStreamRequest {
            stream_id: config.stream.clone(),
            offset: 0,
            max_len: 64,
            now_ms: 0,
        })
        .await
        .expect("read before runtime raft snapshot");
    assert_eq!(source_read.payload, b"snapshot-runtime");
    assert_eq!(source_read.next_offset, append.next_offset);
    let source_metrics = source_runtime.metrics().snapshot();
    trace.push(SimEvent::RuntimeRaftNetworkReadVerified {
        stream: config.stream.clone(),
        next_offset: source_read.next_offset,
        raft_write_many_batches: u64::from(source_metrics.raft_write_many_batches > 0),
        raft_apply_entries: u64::from(source_metrics.raft_apply_entries > 0),
        delivered_rpc_count: usize::from(
            SimTrace::last_recorded()
                .events
                .iter()
                .any(|event| matches!(event, SimEvent::NetworkRpcDelivered { .. })),
        ),
    });

    // Capture source-side stream integrity (live/total setsum) before snapshot
    // so we can assert the snapshot install transfers it intact. Server-side
    // setsum invariance under snapshot install is not otherwise tested in DST;
    // a divergence here is the kind of bug the chaos cluster can only catch
    // probabilistically (and only when its own client setsum tracking is
    // reliable).
    let source_head_pre_snapshot = source_runtime
        .head_stream(HeadStreamRequest {
            stream_id: config.stream.clone(),
            now_ms: 0,
        })
        .await
        .expect("head_stream before runtime raft snapshot");
    let source_live_setsum = source_head_pre_snapshot.integrity.live_setsum.clone();
    let source_total_setsum = source_head_pre_snapshot.integrity.total_setsum.clone();

    let mut snapshot = source_runtime
        .snapshot_group(placement.raft_group_id)
        .await
        .expect("snapshot runtime-owned raft group");
    let snapshot_commit_index = snapshot.group_commit_index;
    let snapshot_stream_count = snapshot.stream_snapshot.streams.len();
    assert!(
        snapshot_commit_index >= append.group_commit_index,
        "runtime raft snapshot should include committed append"
    );
    assert!(
        snapshot
            .stream_snapshot
            .streams
            .iter()
            .any(|entry| entry.metadata.stream_id == config.stream),
        "runtime raft snapshot should contain the stream"
    );
    trace.push(SimEvent::RuntimeRaftSnapshotCaptured {
        stream: config.stream.clone(),
        group_commit_index: snapshot_commit_index,
        stream_count: snapshot_stream_count,
    });
    if corrupt_append_counts {
        for count in &mut snapshot.stream_append_counts {
            if count.stream_id == config.stream {
                count.append_count = 0;
            }
        }
        trace.push(SimEvent::FaultApplied {
            phase: "after_snapshot_capture".to_owned(),
        });
    }

    let mut restore_config = RuntimeConfig::new(1, 1);
    restore_config.threading = RuntimeThreading::HostedTokio;
    let restore_runtime = ShardRuntime::spawn_with_engine_factory(
        restore_config,
        MadsimScopedRaftGroupEngineFactory::new(config.seed ^ 0x7373_686f_745f_696e),
    )
    .expect("spawn hosted restore runtime with raft group engine");
    restore_runtime
        .install_group_snapshot(snapshot)
        .await
        .expect("install runtime raft group snapshot");

    // Setsum invariance across snapshot install: the restored replica must
    // report the same live/total setsum the source had at snapshot time. A
    // divergence here means snapshot bytes carried different integrity state
    // than the source's running aggregate, or the install path re-applied
    // (or skipped) records — exactly the failure mode the chaos cluster has
    // been hitting probabilistically.
    let restored_head = restore_runtime
        .head_stream(HeadStreamRequest {
            stream_id: config.stream.clone(),
            now_ms: 0,
        })
        .await
        .expect("head_stream after runtime raft snapshot install");
    if restored_head.integrity.live_setsum != source_live_setsum
        || restored_head.integrity.total_setsum != source_total_setsum
    {
        let message = format!(
            "setsum diverged after snapshot install: source live={source_live_setsum} \
             restored live={restored_live} source total={source_total_setsum} restored total={restored_total}",
            restored_live = restored_head.integrity.live_setsum,
            restored_total = restored_head.integrity.total_setsum,
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_snapshot_install_setsum_invariance".to_owned(),
            after_event: "runtime_raft_snapshot_captured".to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_snapshot_install_setsum_invariance` failed after `runtime_raft_snapshot_captured`: {message}"
        );
    }

    let restored_read = restore_runtime
        .read_stream(ReadStreamRequest {
            stream_id: config.stream.clone(),
            offset: 0,
            max_len: 64,
            now_ms: 0,
        })
        .await
        .expect("read restored runtime raft snapshot");
    assert_eq!(restored_read.payload, b"snapshot-runtime");
    assert_eq!(restored_read.next_offset, append.next_offset);

    let restore_append_batch = restore_runtime
        .append_batch(AppendBatchRequest::new(config.stream.clone(), vec![
            b"-restored".to_vec(),
        ]))
        .await
        .expect("append after runtime raft snapshot install");
    let restore_append_items = restore_append_batch
        .items
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("runtime raft snapshot restore append batch items");
    let restore_append = restore_append_items
        .last()
        .cloned()
        .expect("runtime raft snapshot restore append response");
    let expected_restore_append_count = 3;
    if restore_append.stream_append_count != expected_restore_append_count {
        let message = format!(
            "restore append count {}, expected {expected_restore_append_count}",
            restore_append.stream_append_count
        );
        SimTrace::record(SimEvent::InvariantFailed {
            invariant: "runtime_raft_snapshot_install_integrity".to_owned(),
            after_event: "runtime_raft_snapshot_captured".to_owned(),
            message: message.clone(),
        });
        panic!(
            "invariant `runtime_raft_snapshot_install_integrity` failed after `runtime_raft_snapshot_captured`: {message}"
        );
    }
    let restored_post_append_read = restore_runtime
        .read_stream(ReadStreamRequest {
            stream_id: config.stream.clone(),
            offset: 0,
            max_len: 64,
            now_ms: 0,
        })
        .await
        .expect("read after runtime raft snapshot restore append");
    assert_eq!(
        restored_post_append_read.payload,
        b"snapshot-runtime-restored"
    );
    assert_eq!(
        restored_post_append_read.next_offset,
        restore_append.next_offset
    );
    trace.push(SimEvent::RuntimeRaftSnapshotInstalledVerified {
        stream: config.stream,
        snapshot_next_offset: restored_read.next_offset,
        post_restore_next_offset: restored_post_append_read.next_offset,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: source_leader_id,
        target_node_id: None,
        appended_log_index: snapshot_commit_index,
        trace,
    }
}

pub(super) async fn run_runtime_raft_network_inner(
    config: ThreeNodeRaftSimConfig,
    options: RuntimeRaftNetworkOptions,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let policy = sim_network_policy();
    let RuntimeRaftNetworkOptions {
        partition_before_append,
        heal_after_lag,
        verify_cold_live_read,
        delay_cold_write_ms,
        delay_cold_read_ms,
        truncate_cold_read_len,
        fail_cold_write,
        retry_cold_write_after_failure,
        retry_cold_read_after_truncate,
        restart_during_cold_flush,
        leader_failover_after_read,
        workload_plan,
    } = options;
    let cold_store = (verify_cold_live_read
        || delay_cold_write_ms.is_some()
        || delay_cold_read_ms.is_some()
        || truncate_cold_read_len.is_some()
        || fail_cold_write)
        .then(|| Arc::new(sim_cold_store()));
    let factory = match cold_store.clone() {
        Some(cold_store) => MadsimRuntimeRaftNetworkFactory::with_cold_store(
            config.seed,
            policy.clone(),
            Some(cold_store),
        ),
        None => MadsimRuntimeRaftNetworkFactory::new(config.seed, policy.clone()),
    };
    let mut runtime_config = RuntimeConfig::new(1, 1);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        factory.clone(),
        cold_store.clone(),
    )
    .expect("spawn hosted runtime with multi-node raft group engine factory");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });

    let streams = runtime_raft_network_streams(&config.stream, workload_plan.stream_count);
    for stream in &streams {
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create stream through runtime-owned multi-node raft engine");
        trace.push(SimEvent::StreamCreated {
            stream: stream.clone(),
        });
    }
    let placement = runtime.locate(&config.stream);
    let leader_id = factory
        .leader_id(placement.raft_group_id)
        .expect("runtime raft network leader id");
    trace.push(SimEvent::RuntimeRaftNetworkBuilt {
        core_count: 1,
        raft_group_count: 1,
        raft_node_count: 3,
        leader_id,
    });
    let mut outcome_leader_id = leader_id;
    let isolated_id = seeded_follower_id(config.seed, leader_id);

    if partition_before_append {
        policy.partition_bidirectional(leader_id, isolated_id);
        trace.push(SimEvent::FaultApplied {
            phase: "before_append".to_owned(),
        });
    }

    let mut expected_streams = Vec::with_capacity(streams.len());
    let mut latest_group_commit_index = 0;
    let leader_raft_for_initial_appends = partition_before_append.then(|| {
        factory
            .raft_handle(leader_id)
            .expect("runtime raft leader handle before partitioned append")
    });
    let mut latest_partition_raft_applied_index = None;
    for (stream_index, stream) in streams.iter().enumerate() {
        let payloads = runtime_raft_network_batch_payloads(
            stream_index,
            0,
            workload_plan.append_batch_len(stream_index),
        );
        let producer = workload_plan
            .producer_sessions
            .then(|| runtime_raft_network_producer(config.seed, stream_index, 0, 0));
        let mut append_request = AppendBatchRequest::new(stream.clone(), payloads.clone());
        append_request.producer = producer.clone();
        let append_batch = runtime
            .append_batch(append_request)
            .await
            .expect("append through runtime-owned multi-node raft engine");
        let append_items = append_batch
            .items
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("runtime raft network append batch items");
        let append = append_items
            .last()
            .cloned()
            .expect("runtime raft network append batch response");
        latest_group_commit_index = append.group_commit_index;
        trace.push(SimEvent::RuntimeRaftEngineAppendCommitted {
            stream: stream.clone(),
            start_offset: append_items
                .first()
                .expect("runtime raft network first append batch response")
                .start_offset,
            next_offset: append.next_offset,
            group_commit_index: append.group_commit_index,
        });
        if let Some(leader_raft) = &leader_raft_for_initial_appends {
            let target = factory
                .log_store_last_log_index(leader_id)
                .await
                .expect("runtime raft leader log store should contain partitioned append");
            wait_raft_applied_index_at_least(
                leader_raft,
                target,
                "runtime raft leader applied partitioned append",
            )
            .await;
            latest_partition_raft_applied_index = Some(target);
        }
        if let Some(producer) = producer {
            let duplicate_payloads =
                runtime_raft_network_duplicate_payloads(stream_index, 0, payloads.len());
            let mut duplicate_request = AppendBatchRequest::new(stream.clone(), duplicate_payloads);
            duplicate_request.producer = Some(producer.clone());
            let duplicate_batch = runtime
                .append_batch(duplicate_request)
                .await
                .expect("duplicate producer append through runtime-owned raft engine");
            let duplicate_items = duplicate_batch
                .items
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("runtime raft duplicate producer append items");
            assert_runtime_raft_producer_duplicate(
                stream,
                &append_items,
                &duplicate_items,
                producer.producer_seq,
            );
            trace.push(SimEvent::RuntimeRaftNetworkProducerDuplicateVerified {
                stream: stream.clone(),
                producer_id: producer.producer_id,
                producer_seq: producer.producer_seq,
                item_count: duplicate_items.len(),
            });
            if let Some(leader_raft) = &leader_raft_for_initial_appends {
                let target = factory
                    .log_store_last_log_index(leader_id)
                    .await
                    .expect("runtime raft leader log store should contain duplicate append");
                wait_raft_applied_index_at_least(
                    leader_raft,
                    target,
                    "runtime raft leader applied duplicate append",
                )
                .await;
                latest_partition_raft_applied_index = Some(target);
            }
        }
        let mut expected_payload = payloads.into_iter().flatten().collect::<Vec<_>>();
        let mut expected_next_offset = append.next_offset;
        if workload_plan.producer_epoch_bumps {
            let epoch_payloads = runtime_raft_network_batch_payloads(stream_index, 2, 1);
            let epoch_producer = runtime_raft_network_producer(config.seed, stream_index, 1, 0);
            let mut epoch_request = AppendBatchRequest::new(stream.clone(), epoch_payloads.clone());
            epoch_request.producer = Some(epoch_producer.clone());
            let epoch_batch = runtime
                .append_batch(epoch_request)
                .await
                .expect("producer epoch bump append through runtime-owned raft engine");
            let epoch_items = epoch_batch
                .items
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("runtime raft producer epoch bump append items");
            let epoch_append = epoch_items
                .last()
                .cloned()
                .expect("runtime raft producer epoch bump response");
            expected_payload.extend(epoch_payloads.into_iter().flatten());
            expected_next_offset = epoch_append.next_offset;
            latest_group_commit_index = epoch_append.group_commit_index;
            trace.push(SimEvent::RuntimeRaftEngineAppendCommitted {
                stream: stream.clone(),
                start_offset: epoch_items
                    .first()
                    .expect("runtime raft producer epoch bump first response")
                    .start_offset,
                next_offset: epoch_append.next_offset,
                group_commit_index: epoch_append.group_commit_index,
            });
            if let Some(leader_raft) = &leader_raft_for_initial_appends {
                let target = factory
                    .log_store_last_log_index(leader_id)
                    .await
                    .expect("runtime raft leader log store should contain epoch bump append");
                wait_raft_applied_index_at_least(
                    leader_raft,
                    target,
                    "runtime raft leader applied producer epoch bump",
                )
                .await;
                latest_partition_raft_applied_index = Some(target);
            }

            let stale_payloads = runtime_raft_network_duplicate_payloads(stream_index, 2, 1);
            let stale_producer = runtime_raft_network_producer(config.seed, stream_index, 0, 1);
            let mut stale_request = AppendBatchRequest::new(stream.clone(), stale_payloads);
            stale_request.producer = Some(stale_producer.clone());
            let stale_result = runtime.append_batch(stale_request).await;
            assert_runtime_raft_producer_stale_epoch(stream, stale_result);
            trace.push(SimEvent::RuntimeRaftNetworkProducerStaleEpochRejected {
                stream: stream.clone(),
                producer_id: stale_producer.producer_id,
                producer_epoch: stale_producer.producer_epoch,
                producer_seq: stale_producer.producer_seq,
            });
            if let Some(leader_raft) = &leader_raft_for_initial_appends {
                let target = factory
                    .log_store_last_log_index(leader_id)
                    .await
                    .expect("runtime raft leader log store should contain stale epoch append");
                wait_raft_applied_index_at_least(
                    leader_raft,
                    target,
                    "runtime raft leader applied stale producer epoch",
                )
                .await;
                latest_partition_raft_applied_index = Some(target);
            }
        }
        if workload_plan.concurrent_producers {
            let producer_count = 2;
            let mut tasks = Vec::with_capacity(producer_count);
            for producer_index in 0..producer_count {
                let runtime = runtime.clone();
                let stream = stream.clone();
                let payloads =
                    runtime_raft_network_batch_payloads(stream_index, 3 + producer_index, 1);
                let producer = runtime_raft_network_producer_with_lane(
                    config.seed,
                    stream_index,
                    producer_index + 1,
                    0,
                    0,
                );
                tasks.push(madsim::task::spawn(async move {
                    let mut request = AppendBatchRequest::new(stream, payloads.clone());
                    request.producer = Some(producer.clone());
                    let batch = runtime
                        .append_batch(request)
                        .await
                        .expect("concurrent producer append through runtime-owned raft engine");
                    let items = batch
                        .items
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .expect("runtime raft concurrent producer append items");
                    let append = items
                        .last()
                        .cloned()
                        .expect("runtime raft concurrent producer append response");
                    (payloads, producer, items, append)
                }));
            }

            let mut concurrent_appends = Vec::with_capacity(producer_count);
            for task in tasks {
                concurrent_appends.push(task.await.expect("join concurrent producer append task"));
            }
            concurrent_appends.sort_by_key(|(_, _, items, _)| {
                items
                    .first()
                    .expect("runtime raft concurrent producer first response")
                    .start_offset
            });

            let concurrent_start_offset = concurrent_appends
                .first()
                .and_then(|(_, _, items, _)| items.first())
                .expect("runtime raft concurrent producers should append at least one item")
                .start_offset;
            let mut previous_next_offset = expected_next_offset;
            for (payloads, producer, items, append) in &concurrent_appends {
                let first = items
                    .first()
                    .expect("runtime raft concurrent producer first response");
                assert_eq!(
                    first.start_offset, previous_next_offset,
                    "runtime raft concurrent producer {} for {stream} did not append at the expected contiguous offset",
                    producer.producer_id
                );
                assert!(
                    items.iter().all(|item| !item.deduplicated),
                    "runtime raft concurrent producer {} for {stream} unexpectedly deduplicated a first append",
                    producer.producer_id
                );
                previous_next_offset = append.next_offset;
                expected_payload.extend(payloads.iter().flatten().copied());
                expected_next_offset = append.next_offset;
                latest_group_commit_index = append.group_commit_index;
            }
            trace.push(SimEvent::RuntimeRaftNetworkConcurrentProducersVerified {
                stream: stream.clone(),
                producer_count,
                start_offset: concurrent_start_offset,
                next_offset: expected_next_offset,
            });
            trace.push(SimEvent::RuntimeRaftEngineAppendCommitted {
                stream: stream.clone(),
                start_offset: concurrent_start_offset,
                next_offset: expected_next_offset,
                group_commit_index: latest_group_commit_index,
            });
            if let Some(leader_raft) = &leader_raft_for_initial_appends {
                let target = factory
                    .log_store_last_log_index(leader_id)
                    .await
                    .expect("runtime raft leader log store should contain concurrent producers");
                wait_raft_applied_index_at_least(
                    leader_raft,
                    target,
                    "runtime raft leader applied concurrent producers",
                )
                .await;
                latest_partition_raft_applied_index = Some(target);
            }
        }
        expected_streams.push((stream.clone(), expected_payload, expected_next_offset));
    }

    if partition_before_append {
        let isolated_raft = factory
            .follower_raft_handle(isolated_id)
            .expect("isolated follower raft handle");
        let leader_raft = leader_raft_for_initial_appends
            .as_ref()
            .expect("runtime raft leader handle after append");
        let mut latest_raft_applied_index = latest_partition_raft_applied_index
            .expect("runtime raft leader should have applied appended entries");
        let isolated_wait = isolated_raft
            .wait(Some(Duration::from_millis(50)))
            .applied_index_at_least(
                Some(latest_raft_applied_index),
                "runtime raft isolated follower should lag",
            )
            .await;
        assert!(
            isolated_wait.is_err(),
            "runtime raft isolated follower should not apply before heal"
        );
        trace.push(SimEvent::IsolatedFollowerLagged {
            node_id: isolated_id,
            log_index: latest_raft_applied_index,
        });
        if !heal_after_lag {
            let message = format!(
                "runtime raft node {isolated_id} remained partitioned at log index {}",
                latest_raft_applied_index
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: "runtime_raft_network_follower_catchup".to_owned(),
                after_event: "isolated_follower_lagged".to_owned(),
                message: message.clone(),
            });
            panic!(
                "invariant `runtime_raft_network_follower_catchup` failed after `isolated_follower_lagged`: {message}"
            );
        }
        policy.heal_bidirectional(leader_id, isolated_id);
        trace.push(SimEvent::FaultApplied {
            phase: "after_isolated_lag".to_owned(),
        });
        for attempt in 0..5 {
            if isolated_raft
                .wait(Some(Duration::from_millis(100)))
                .applied_index_at_least(
                    Some(latest_raft_applied_index),
                    "runtime raft healed follower catches up",
                )
                .await
                .is_ok()
            {
                break;
            }
            leader_raft
                .trigger()
                .heartbeat()
                .await
                .expect("trigger heartbeat while waiting for runtime raft healed follower");
            SimTrace::record(SimEvent::HeartbeatTriggered {
                node_id: leader_id,
                reason: "waiting for runtime raft healed follower".to_owned(),
                attempt,
            });
            madsim::time::sleep(Duration::from_millis(100)).await;
        }
        if isolated_raft
            .wait(Some(Duration::from_millis(100)))
            .applied_index_at_least(
                Some(latest_raft_applied_index),
                "runtime raft healed follower catches up before recovery probe",
            )
            .await
            .is_err()
        {
            let (stream, expected_payload, expected_next_offset) = expected_streams
                .first_mut()
                .expect("runtime raft network primary stream expectation");
            let payloads = vec![b"recovery-probe;".to_vec()];
            let recovery_batch = runtime
                .append_batch(AppendBatchRequest::new(stream.clone(), payloads.clone()))
                .await
                .expect("append runtime raft recovery probe after heal");
            let recovery_items = recovery_batch
                .items
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("runtime raft recovery probe append items");
            let recovery_append = recovery_items
                .last()
                .cloned()
                .expect("runtime raft recovery probe append response");
            expected_payload.extend(payloads.into_iter().flatten());
            *expected_next_offset = recovery_append.next_offset;
            latest_group_commit_index = recovery_append.group_commit_index;
            latest_raft_applied_index = factory
                .log_store_last_log_index(leader_id)
                .await
                .expect("runtime raft leader log store should contain recovery probe");
            wait_raft_applied_index_at_least(
                leader_raft,
                latest_raft_applied_index,
                "runtime raft leader applied recovery probe",
            )
            .await;
            trace.push(SimEvent::RuntimeRaftEngineAppendCommitted {
                stream: stream.clone(),
                start_offset: recovery_items
                    .first()
                    .expect("runtime raft recovery probe first response")
                    .start_offset,
                next_offset: recovery_append.next_offset,
                group_commit_index: recovery_append.group_commit_index,
            });
        }
        if let Err(err) = isolated_raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(latest_raft_applied_index),
                "runtime raft healed follower catches up",
            )
            .await
        {
            let invariant = if workload_plan.stream_count > 1 {
                "runtime_raft_network_multistream_follower_catchup"
            } else {
                "runtime_raft_network_follower_catchup"
            };
            let message = format!(
                "runtime raft healed follower {isolated_id} did not catch up to raft log index {latest_raft_applied_index}: {err}"
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: invariant.to_owned(),
                after_event: "isolated_follower_lagged".to_owned(),
                message: message.clone(),
            });
            panic!("invariant `{invariant}` failed after `isolated_follower_lagged`: {message}");
        }
        trace.push(SimEvent::FollowerCaughtUp {
            node_id: isolated_id,
            log_index: latest_raft_applied_index,
        });
    }

    let metrics = runtime.metrics().snapshot();
    assert!(
        metrics.raft_write_many_batches > 0,
        "runtime raft network scenario should submit writes through OpenRaft"
    );
    assert!(
        metrics.raft_apply_entries >= 2,
        "runtime raft network scenario should apply OpenRaft entries"
    );
    let delivered_rpc_count = SimTrace::last_recorded()
        .events
        .iter()
        .filter(|event| matches!(event, SimEvent::NetworkRpcDelivered { .. }))
        .count();
    assert!(
        delivered_rpc_count > 0,
        "runtime raft network scenario should deliver in-process Raft RPCs"
    );
    for (stream, expected_payload, expected_next_offset) in &expected_streams {
        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: expected_payload.len().max(64),
                now_ms: 0,
            })
            .await
            .expect("read through runtime-owned multi-node raft engine");
        let mut expected_for_read = expected_payload.clone();
        if workload_plan.corrupt_read_expectation {
            expected_for_read.push(b'!');
        }
        assert_runtime_raft_read_consistency(
            stream,
            &read.payload,
            &expected_for_read,
            read.next_offset,
            *expected_next_offset + u64::from(workload_plan.corrupt_read_expectation),
            "runtime_raft_network_read_verified",
        );
        trace.push(SimEvent::RuntimeRaftNetworkReadVerified {
            stream: stream.clone(),
            next_offset: read.next_offset,
            raft_write_many_batches: metrics.raft_write_many_batches,
            raft_apply_entries: metrics.raft_apply_entries,
            delivered_rpc_count: usize::from(delivered_rpc_count > 0),
        });
        if workload_plan.partial_reads {
            verify_runtime_raft_partial_read(
                &runtime,
                stream,
                expected_payload,
                "runtime_raft_network_read_verified",
                workload_plan.corrupt_partial_read_expectation,
                &mut trace,
            )
            .await;
        }
        if workload_plan.tail_reads {
            verify_runtime_raft_tail_read(
                &runtime,
                stream,
                *expected_next_offset,
                "runtime_raft_network_read_verified",
                workload_plan.corrupt_tail_read_expectation,
                &mut trace,
            )
            .await;
        }
    }

    let mut failover_target_node_id = None;
    if leader_failover_after_read {
        let old_leader_id = factory
            .unregister_current_leader(placement.raft_group_id)
            .expect("unregister runtime raft current leader");
        runtime
            .shutdown_group_engine_for_simulation(placement)
            .await
            .expect("shutdown runtime-owned raft leader engine");
        trace.push(SimEvent::FaultApplied {
            phase: "after_runtime_read".to_owned(),
        });
        trace.push(SimEvent::NodeStopped {
            node_id: old_leader_id,
        });
        trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached {
            stage: "old_leader_stopped".to_owned(),
            old_leader_id,
            current_leader_id: None,
            log_index: None,
        });

        let (new_leader_id, new_engine) = factory
            .take_current_leader_engine(placement.raft_group_id)
            .await
            .expect("take runtime raft replacement leader engine");
        outcome_leader_id = new_leader_id;
        trace.push(SimEvent::LeaderElected {
            leader_id: new_leader_id,
        });
        runtime
            .install_group_engine_for_simulation(placement, new_engine)
            .await
            .expect("install runtime raft replacement leader engine");
        trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached {
            stage: "replacement_leader_installed".to_owned(),
            old_leader_id,
            current_leader_id: Some(new_leader_id),
            log_index: None,
        });
        let new_leader_raft = factory
            .raft_handle(new_leader_id)
            .expect("runtime raft new leader handle before failover append");
        let mut failover_raft_applied_index = None;

        for (stream_index, (stream, expected_payload, expected_next_offset)) in
            expected_streams.iter_mut().enumerate()
        {
            let payloads = runtime_raft_network_batch_payloads(
                stream_index,
                1,
                workload_plan.failover_batch_len(stream_index),
            );
            let payload_count = payloads.len();
            let producer = workload_plan.producer_sessions.then(|| {
                runtime_raft_network_producer(
                    config.seed,
                    stream_index,
                    u64::from(workload_plan.producer_epoch_bumps),
                    1,
                )
            });
            let mut failover_request = AppendBatchRequest::new(stream.clone(), payloads.clone());
            failover_request.producer = producer.clone();
            let failover_batch = runtime
                .append_batch(failover_request)
                .await
                .expect("append through replacement runtime-owned raft leader");
            let failover_items = failover_batch
                .items
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .expect("runtime raft failover append batch items");
            let failover_append = failover_items
                .last()
                .cloned()
                .expect("runtime raft failover append response");
            expected_payload.extend(payloads.into_iter().flatten());
            *expected_next_offset = failover_append.next_offset;
            latest_group_commit_index = failover_append.group_commit_index;
            let target = factory
                .log_store_last_log_index(new_leader_id)
                .await
                .expect("runtime raft new leader log store should contain failover append");
            wait_raft_applied_index_at_least(
                &new_leader_raft,
                target,
                "runtime raft new leader applied failover append",
            )
            .await;
            failover_raft_applied_index = Some(target);
            trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverVerified {
                stream: stream.clone(),
                old_leader_id,
                new_leader_id,
                next_offset: failover_append.next_offset,
                group_commit_index: failover_append.group_commit_index,
            });
            if let Some(producer) = producer {
                let duplicate_payloads =
                    runtime_raft_network_duplicate_payloads(stream_index, 1, payload_count);
                let mut duplicate_request =
                    AppendBatchRequest::new(stream.clone(), duplicate_payloads);
                duplicate_request.producer = Some(producer.clone());
                let duplicate_batch = runtime
                    .append_batch(duplicate_request)
                    .await
                    .expect("duplicate producer append through replacement raft leader");
                let duplicate_items = duplicate_batch
                    .items
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .expect("runtime raft failover duplicate producer append items");
                assert_runtime_raft_producer_duplicate(
                    stream,
                    &failover_items,
                    &duplicate_items,
                    producer.producer_seq,
                );
                trace.push(SimEvent::RuntimeRaftNetworkProducerDuplicateVerified {
                    stream: stream.clone(),
                    producer_id: producer.producer_id,
                    producer_seq: producer.producer_seq,
                    item_count: duplicate_items.len(),
                });
                let target = factory
                    .log_store_last_log_index(new_leader_id)
                    .await
                    .expect("runtime raft new leader log store should contain duplicate append");
                wait_raft_applied_index_at_least(
                    &new_leader_raft,
                    target,
                    "runtime raft new leader applied duplicate append",
                )
                .await;
                failover_raft_applied_index = Some(target);
            }
        }

        factory
            .restart_follower(old_leader_id)
            .await
            .expect("restart old runtime raft leader as follower");
        trace.push(SimEvent::FaultApplied {
            phase: "after_failover_append".to_owned(),
        });
        trace.push(SimEvent::NodeRestarted {
            node_id: old_leader_id,
        });
        let restarted_raft = factory
            .follower_raft_handle(old_leader_id)
            .expect("restarted old runtime raft leader handle");
        let latest_raft_applied_index = failover_raft_applied_index
            .expect("runtime raft new leader should apply failover appends");
        trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached {
            stage: "failover_appends_applied".to_owned(),
            old_leader_id,
            current_leader_id: Some(new_leader_id),
            log_index: Some(latest_raft_applied_index),
        });
        for attempt in 0..50 {
            if restarted_raft
                .wait(Some(Duration::from_millis(100)))
                .applied_index_at_least(
                    Some(latest_raft_applied_index),
                    "runtime raft old leader catches up after failover",
                )
                .await
                .is_ok()
            {
                break;
            }
            new_leader_raft
                .trigger()
                .heartbeat()
                .await
                .expect("trigger heartbeat while waiting for runtime raft old leader");
            SimTrace::record(SimEvent::HeartbeatTriggered {
                node_id: new_leader_id,
                reason: "waiting for runtime raft old leader after failover".to_owned(),
                attempt,
            });
            madsim::time::sleep(Duration::from_millis(100)).await;
        }
        if let Err(err) = restarted_raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(
                Some(latest_raft_applied_index),
                "runtime raft old leader catches up after failover",
            )
            .await
        {
            let invariant = if workload_plan.stream_count > 1 {
                "runtime_raft_network_multistream_leader_failover_catchup"
            } else {
                "runtime_raft_network_leader_failover_catchup"
            };
            let message = format!(
                "runtime raft old leader {old_leader_id} did not catch up to raft log index {latest_raft_applied_index}: {err}"
            );
            SimTrace::record(SimEvent::InvariantFailed {
                invariant: invariant.to_owned(),
                after_event: "runtime_raft_network_leader_failover_verified".to_owned(),
                message: message.clone(),
            });
            panic!(
                "invariant `{invariant}` failed after `runtime_raft_network_leader_failover_verified`: {message}"
            );
        }
        trace.push(SimEvent::FollowerCaughtUp {
            node_id: old_leader_id,
            log_index: latest_raft_applied_index,
        });
        trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached {
            stage: "old_leader_caught_up".to_owned(),
            old_leader_id,
            current_leader_id: Some(new_leader_id),
            log_index: Some(latest_raft_applied_index),
        });

        for (stream, expected_payload, expected_next_offset) in &expected_streams {
            let failover_read = runtime
                .read_stream(ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: expected_payload.len().max(64),
                    now_ms: 0,
                })
                .await
                .expect("read after runtime-owned raft leader failover");
            let mut expected_for_failover_read = expected_payload.clone();
            if workload_plan.corrupt_leader_failover_read_expectation {
                expected_for_failover_read.push(b'!');
            }
            assert_runtime_raft_leader_failover_read_consistency(
                stream,
                &failover_read.payload,
                &expected_for_failover_read,
                failover_read.next_offset,
                *expected_next_offset
                    + u64::from(workload_plan.corrupt_leader_failover_read_expectation),
            );
            trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverReadVerified {
                stream: stream.clone(),
                next_offset: failover_read.next_offset,
            });
            if workload_plan.partial_reads {
                verify_runtime_raft_partial_read(
                    &runtime,
                    stream,
                    expected_payload,
                    "runtime_raft_network_leader_failover_read_verified",
                    workload_plan.corrupt_partial_read_expectation,
                    &mut trace,
                )
                .await;
            }
            if workload_plan.tail_reads {
                verify_runtime_raft_tail_read(
                    &runtime,
                    stream,
                    *expected_next_offset,
                    "runtime_raft_network_leader_failover_read_verified",
                    workload_plan.corrupt_tail_read_expectation,
                    &mut trace,
                )
                .await;
            }
        }
        failover_target_node_id = Some(old_leader_id);
    }

    if workload_plan.close_streams {
        let close_after_event = if leader_failover_after_read {
            "runtime_raft_network_leader_failover_read_verified"
        } else {
            "runtime_raft_network_read_verified"
        };
        for (stream, expected_payload, expected_next_offset) in &expected_streams {
            let close = verify_runtime_raft_close_stream(
                &runtime,
                stream,
                expected_payload,
                *expected_next_offset,
                close_after_event,
                workload_plan.corrupt_close_state_expectation,
                &mut trace,
            )
            .await;
            latest_group_commit_index = close.group_commit_index;
        }
    }

    if verify_cold_live_read {
        let cold_restart_node_id = restart_during_cold_flush.then(|| {
            factory
                .retained_follower_id_prefer(isolated_id)
                .expect("runtime raft retained follower for cold flush restart")
        });
        if restart_during_cold_flush {
            let cold_restart_node_id =
                cold_restart_node_id.expect("runtime raft cold restart node id");
            factory
                .stop_follower(cold_restart_node_id)
                .await
                .expect("stop runtime raft follower before cold flush");
            trace.push(SimEvent::NodeStopped {
                node_id: cold_restart_node_id,
            });
        }
        let current_leader_raft = factory
            .raft_handle(outcome_leader_id)
            .expect("runtime raft current leader handle before cold flush");
        let cold_write_delay_consumed = if let Some(delay_ms) = delay_cold_write_ms {
            let cold_store = cold_store
                .as_ref()
                .expect("runtime raft network cold write delay requires cold store")
                .clone();
            let delay = Duration::from_millis(delay_ms);
            let consumed = Arc::new(Mutex::new(false));
            let consumed_for_policy = Arc::clone(&consumed);
            cold_store.set_fault_policy(move |context| {
                if context.operation != ColdStoreOperation::WriteChunk {
                    return None;
                }
                let mut consumed = consumed_for_policy
                    .lock()
                    .expect("runtime raft network cold write delay policy mutex");
                if *consumed {
                    return None;
                }
                *consumed = true;
                Some(ColdStoreFaultEffect::delay(delay))
            });
            trace.push(SimEvent::FaultApplied {
                phase: "before_cold_write".to_owned(),
            });
            Some(consumed)
        } else {
            None
        };
        let cold_write_fault_consumed = if fail_cold_write {
            assert!(
                delay_cold_write_ms.is_none(),
                "runtime raft network cold write delay and write fault cannot share one one-shot policy"
            );
            let cold_store = cold_store
                .as_ref()
                .expect("runtime raft network cold write fault requires cold store")
                .clone();
            let consumed = Arc::new(Mutex::new(false));
            let consumed_for_policy = Arc::clone(&consumed);
            cold_store.set_fault_policy(move |context| {
                if context.operation != ColdStoreOperation::WriteChunk {
                    return None;
                }
                let mut consumed = consumed_for_policy
                    .lock()
                    .expect("runtime raft network cold write fault policy mutex");
                if *consumed {
                    return None;
                }
                *consumed = true;
                Some(ColdStoreFaultEffect::fail(
                    "seeded runtime raft network cold write fault",
                ))
            });
            trace.push(SimEvent::FaultApplied {
                phase: "before_cold_write".to_owned(),
            });
            Some(consumed)
        } else {
            None
        };
        let flush_request = PlanGroupColdFlushRequest {
            min_hot_bytes: 1,
            max_flush_bytes: 8,
        };
        let max_flush_candidates = expected_streams.len() * 4;
        if let Some(old_leader_id) = failover_target_node_id {
            trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached {
                stage: "cold_flush_started_after_failover".to_owned(),
                old_leader_id,
                current_leader_id: Some(outcome_leader_id),
                log_index: None,
            });
        }
        let flushed_responses = match runtime
            .flush_cold_group_batch_once(
                placement.raft_group_id,
                flush_request.clone(),
                max_flush_candidates,
            )
            .await
        {
            Ok(flushed_responses) => flushed_responses,
            Err(err) => {
                if let Some(consumed) = cold_write_fault_consumed.as_ref() {
                    assert!(
                        *consumed
                            .lock()
                            .expect("runtime raft network cold write fault consumed mutex"),
                        "runtime raft network cold write fault should be consumed"
                    );
                    let metrics = runtime.metrics().snapshot();
                    let message = format!(
                        "runtime raft network cold flush failed after write fault: {err}; uploads {}; publishes {}",
                        metrics.cold_flush_uploads, metrics.cold_flush_publishes
                    );
                    if retry_cold_write_after_failure {
                        trace.push(SimEvent::RuntimeRaftNetworkColdWriteFaultRecovered {
                            stream_count: expected_streams.len(),
                            upload_count_before_retry: metrics.cold_flush_uploads,
                            publish_count_before_retry: metrics.cold_flush_publishes,
                        });
                        runtime
                            .flush_cold_group_batch_once(
                                placement.raft_group_id,
                                flush_request,
                                max_flush_candidates,
                            )
                            .await
                            .unwrap_or_else(|retry_err| {
                                panic!(
                                    "retry runtime raft cold flush after write fault failed: {retry_err}"
                                )
                            })
                    } else {
                        SimTrace::record(SimEvent::InvariantFailed {
                            invariant: "runtime_raft_network_cold_live_write_integrity".to_owned(),
                            after_event: "runtime_raft_network_read_verified".to_owned(),
                            message: message.clone(),
                        });
                        panic!(
                            "invariant `runtime_raft_network_cold_live_write_integrity` failed after `runtime_raft_network_read_verified`: {message}"
                        );
                    }
                } else {
                    panic!("flush cold chunk through runtime-owned multi-node raft engine: {err}");
                }
            }
        };
        assert!(!flushed_responses.is_empty());
        let cold_flush_raft_applied_index = factory
            .log_store_last_log_index(outcome_leader_id)
            .await
            .expect("runtime raft current leader log store should contain cold flush");
        wait_raft_applied_index_at_least(
            &current_leader_raft,
            cold_flush_raft_applied_index,
            "runtime raft leader applied cold flush",
        )
        .await;
        if let Some(old_leader_id) = failover_target_node_id {
            trace.push(SimEvent::RuntimeRaftNetworkLeaderFailoverStageReached {
                stage: "cold_flush_applied_after_failover".to_owned(),
                old_leader_id,
                current_leader_id: Some(outcome_leader_id),
                log_index: Some(cold_flush_raft_applied_index),
            });
        }
        if restart_during_cold_flush {
            let cold_restart_node_id =
                cold_restart_node_id.expect("runtime raft cold restart node id");
            factory
                .restart_follower(cold_restart_node_id)
                .await
                .expect("restart runtime raft follower after cold flush");
            trace.push(SimEvent::NodeRestarted {
                node_id: cold_restart_node_id,
            });
            let restarted_raft = factory
                .follower_raft_handle(cold_restart_node_id)
                .expect("restarted runtime raft follower handle");
            for attempt in 0..50 {
                if restarted_raft
                    .wait(Some(Duration::from_millis(100)))
                    .applied_index_at_least(
                        Some(cold_flush_raft_applied_index),
                        "runtime raft restarted follower catches up after cold flush",
                    )
                    .await
                    .is_ok()
                {
                    break;
                }
                current_leader_raft
                    .trigger()
                    .heartbeat()
                    .await
                    .expect("trigger heartbeat while waiting for runtime raft cold-flush follower");
                SimTrace::record(SimEvent::HeartbeatTriggered {
                    node_id: outcome_leader_id,
                    reason: "waiting for runtime raft cold-flush follower".to_owned(),
                    attempt,
                });
                madsim::time::sleep(Duration::from_millis(100)).await;
            }
            restarted_raft
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(
                    Some(cold_flush_raft_applied_index),
                    "runtime raft restarted follower catches up after cold flush",
                )
                .await
                .expect("wait for restarted runtime raft follower after cold flush");
            trace.push(SimEvent::FollowerCaughtUp {
                node_id: cold_restart_node_id,
                log_index: cold_flush_raft_applied_index,
            });
        }

        let delay_consumed = if let Some(delay_ms) = delay_cold_read_ms {
            let cold_store = cold_store
                .as_ref()
                .expect("runtime raft network cold read delay requires cold store")
                .clone();
            let delay = Duration::from_millis(delay_ms);
            let consumed = Arc::new(Mutex::new(false));
            let consumed_for_policy = Arc::clone(&consumed);
            cold_store.set_fault_policy(move |context| {
                if context.operation != ColdStoreOperation::ReadObjectRange {
                    return None;
                }
                let mut consumed = consumed_for_policy
                    .lock()
                    .expect("runtime raft network cold read delay policy mutex");
                if *consumed {
                    return None;
                }
                *consumed = true;
                Some(ColdStoreFaultEffect::delay(delay))
            });
            trace.push(SimEvent::FaultApplied {
                phase: "before_cold_read".to_owned(),
            });
            Some(consumed)
        } else {
            None
        };
        let truncate_consumed = if let Some(returned_len) = truncate_cold_read_len {
            let cold_store = cold_store
                .as_ref()
                .expect("runtime raft network cold live fault requires cold store")
                .clone();
            let consumed = Arc::new(Mutex::new(false));
            let consumed_for_policy = Arc::clone(&consumed);
            cold_store.set_fault_policy(move |context| {
                if context.operation != ColdStoreOperation::ReadObjectRange {
                    return None;
                }
                let mut consumed = consumed_for_policy
                    .lock()
                    .expect("runtime raft network cold read truncate policy mutex");
                if *consumed {
                    return None;
                }
                *consumed = true;
                Some(ColdStoreFaultEffect::truncate_read_to(returned_len))
            });
            trace.push(SimEvent::FaultApplied {
                phase: "before_cold_read".to_owned(),
            });
            Some(consumed)
        } else {
            None
        };
        let metrics = runtime.metrics().snapshot();
        assert_eq!(metrics.cold_flush_uploads, flushed_responses.len() as u64);
        assert_eq!(metrics.cold_flush_publishes, flushed_responses.len() as u64);
        if let (Some(delay_ms), Some(consumed)) =
            (delay_cold_write_ms, cold_write_delay_consumed.as_ref())
        {
            assert!(
                *consumed
                    .lock()
                    .expect("runtime raft network cold write delay consumed mutex"),
                "runtime raft network cold write delay fault should be consumed"
            );
            trace.push(SimEvent::RuntimeRaftNetworkColdWriteDelayVerified {
                stream_count: expected_streams.len(),
                delay_ms,
                upload_count: metrics.cold_flush_uploads,
                publish_count: metrics.cold_flush_publishes,
            });
        }
        let mut delay_verified = false;
        for (stream, expected_payload, expected_next_offset) in &expected_streams {
            let read_request = ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: expected_payload.len().max(64),
                now_ms: 0,
            };
            let cold_live_read = match runtime.read_stream(read_request.clone()).await {
                Ok(read) => read,
                Err(err) => {
                    if let Some(consumed) = truncate_consumed.as_ref() {
                        assert!(
                            *consumed
                                .lock()
                                .expect("runtime raft network cold read truncate consumed mutex"),
                            "runtime raft network cold read truncate fault should be consumed"
                        );
                    }
                    if retry_cold_read_after_truncate {
                        trace.push(SimEvent::RuntimeRaftNetworkColdReadFaultRecovered {
                            stream: stream.clone(),
                            returned_len: truncate_cold_read_len.unwrap_or_default(),
                        });
                        runtime.read_stream(read_request).await.unwrap_or_else(|retry_err| {
                            panic!(
                                "retry runtime raft network cold/live read for stream {stream} after truncate failed: {retry_err}; first error: {err}"
                            )
                        })
                    } else {
                        let message = format!(
                            "runtime raft network cold/live read for stream {stream} failed: {err}"
                        );
                        let invariant = if leader_failover_after_read {
                            "runtime_raft_network_leader_failover_cold_live_read_integrity"
                        } else {
                            "runtime_raft_network_cold_live_read_integrity"
                        };
                        SimTrace::record(SimEvent::InvariantFailed {
                            invariant: invariant.to_owned(),
                            after_event: "runtime_raft_network_read_verified".to_owned(),
                            message: message.clone(),
                        });
                        panic!(
                            "invariant `{invariant}` failed after `runtime_raft_network_read_verified`: {message}"
                        );
                    }
                }
            };
            if let (Some(delay_ms), Some(consumed)) = (delay_cold_read_ms, delay_consumed.as_ref())
                && !delay_verified
            {
                assert!(
                    *consumed
                        .lock()
                        .expect("runtime raft network cold read delay consumed mutex"),
                    "runtime raft network cold read delay fault should be consumed"
                );
                trace.push(SimEvent::RuntimeRaftNetworkColdReadDelayVerified {
                    stream: stream.clone(),
                    delay_ms,
                });
                delay_verified = true;
            }
            assert_cold_live_read_consistency(
                leader_id,
                stream,
                &cold_live_read.payload,
                expected_payload,
                cold_live_read.next_offset,
                *expected_next_offset,
                "runtime_raft_network_cold_live_read_verified",
            );
            let expected_closed =
                workload_plan.close_streams && !workload_plan.corrupt_close_state_expectation;
            if workload_plan.close_streams && cold_live_read.closed != expected_closed {
                let message = format!(
                    "cold/live read for closed stream {stream} returned closed={} at next_offset {}, expected closed={expected_closed}",
                    cold_live_read.closed, cold_live_read.next_offset
                );
                SimTrace::record(SimEvent::InvariantFailed {
                    invariant: "runtime_raft_network_close_state".to_owned(),
                    after_event: "runtime_raft_network_cold_live_read_verified".to_owned(),
                    message: message.clone(),
                });
                panic!(
                    "invariant `runtime_raft_network_close_state` failed after `runtime_raft_network_cold_live_read_verified`: {message}"
                );
            }
            trace.push(SimEvent::RuntimeRaftNetworkColdLiveReadVerified {
                stream: stream.clone(),
                next_offset: cold_live_read.next_offset,
                flushed_count: flushed_responses.len(),
                upload_count: metrics.cold_flush_uploads,
                publish_count: metrics.cold_flush_publishes,
            });
            if workload_plan.partial_reads {
                verify_runtime_raft_partial_read(
                    &runtime,
                    stream,
                    expected_payload,
                    "runtime_raft_network_cold_live_read_verified",
                    workload_plan.corrupt_partial_read_expectation,
                    &mut trace,
                )
                .await;
            }
            if workload_plan.tail_reads {
                verify_runtime_raft_tail_read(
                    &runtime,
                    stream,
                    *expected_next_offset,
                    "runtime_raft_network_cold_live_read_verified",
                    workload_plan.corrupt_tail_read_expectation,
                    &mut trace,
                )
                .await;
            }
        }
        if leader_failover_after_read {
            trace.push(
                SimEvent::RuntimeRaftNetworkLeaderFailoverColdLiveReadVerified {
                    stream_count: expected_streams.len(),
                    old_leader_id: failover_target_node_id
                        .expect("runtime raft leader failover should set old leader id"),
                    current_leader_id: outcome_leader_id,
                    flushed_count: flushed_responses.len(),
                },
            );
        }
    }

    if workload_plan.publish_snapshots {
        let snapshot_after_event = if verify_cold_live_read {
            "runtime_raft_network_cold_live_read_verified"
        } else if workload_plan.close_streams {
            "runtime_raft_network_close_verified"
        } else if leader_failover_after_read {
            "runtime_raft_network_leader_failover_read_verified"
        } else {
            "runtime_raft_network_read_verified"
        };
        for (stream_index, (stream, _expected_payload, expected_next_offset)) in
            expected_streams.iter().enumerate()
        {
            let publish = verify_runtime_raft_snapshot_publish(
                &runtime,
                stream_index,
                stream,
                *expected_next_offset,
                snapshot_after_event,
                workload_plan.corrupt_snapshot_expectation,
                &mut trace,
            )
            .await;
            latest_group_commit_index = publish.group_commit_index;
        }
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: outcome_leader_id,
        target_node_id: failover_target_node_id
            .or_else(|| partition_before_append.then_some(isolated_id)),
        appended_log_index: latest_group_commit_index,
        trace,
    }
}

pub(super) async fn run_runtime_multi_client_actors_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let mut runtime_config = RuntimeConfig::new(2, 4);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    runtime_config.mailbox_capacity = 4;
    let runtime = ShardRuntime::spawn(runtime_config).expect("spawn hosted runtime actors");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::RuntimeMultiClientActorsBuilt {
        stream_count: 4,
        core_count: 2,
        raft_group_count: 4,
    });

    let streams = choose_runtime_streams_spanning_placement(&runtime, &config.stream, 4);
    let mut cores = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for stream in &streams {
        let placement = runtime.locate(stream);
        cores.insert(placement.core_id.0);
        groups.insert(placement.raft_group_id.0);
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create multi-client stream through hosted runtime actors");
        trace.push(SimEvent::RuntimeMultiClientStreamCreated {
            stream: stream.clone(),
            core_id: placement.core_id.0,
            raft_group_id: placement.raft_group_id.0,
        });
    }
    assert!(
        cores.len() >= 2,
        "multi-client runtime scenario should span at least two cores"
    );
    assert!(
        groups.len() >= 2,
        "multi-client runtime scenario should span at least two raft groups"
    );

    let mut tasks = Vec::new();
    for (client_id, stream) in streams.iter().cloned().enumerate() {
        let runtime = runtime.clone();
        tasks.push(madsim::task::spawn(async move {
            let first_payload = format!("client-{client_id}-a").into_bytes();
            let second_payload = format!("client-{client_id}-b").into_bytes();
            madsim::time::sleep(Duration::from_millis(
                10 * u64::try_from(client_id + 1).expect("client id fits u64"),
            ))
            .await;

            let first = runtime
                .append(AppendRequest::from_bytes(
                    stream.clone(),
                    first_payload.clone(),
                ))
                .await
                .expect("first multi-client append");
            let first_read = runtime
                .read_stream(ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 64,
                    now_ms: 0,
                })
                .await
                .expect("read after first multi-client append");
            assert_eq!(first_read.payload, first_payload);

            madsim::time::sleep(Duration::from_millis(
                5 * u64::try_from(client_id + 1).expect("client id fits u64"),
            ))
            .await;
            let second = runtime
                .append(AppendRequest::from_bytes(
                    stream.clone(),
                    second_payload.clone(),
                ))
                .await
                .expect("second multi-client append");
            let mut expected = first_payload;
            expected.extend_from_slice(&second_payload);
            let second_read = runtime
                .read_stream(ReadStreamRequest {
                    stream_id: stream.clone(),
                    offset: 0,
                    max_len: 128,
                    now_ms: 0,
                })
                .await
                .expect("read after second multi-client append");
            assert_eq!(second_read.payload, expected);
            (
                stream,
                client_id,
                first.start_offset,
                first.next_offset,
                second.start_offset,
                second.next_offset,
                second.group_commit_index,
                first_read.next_offset,
                second_read.next_offset,
                expected.len(),
            )
        }));
    }

    let mut max_commit_index = 0;
    for task in tasks {
        let (
            stream,
            client_id,
            first_start_offset,
            first_next_offset,
            second_start_offset,
            second_next_offset,
            second_commit_index,
            first_read_next_offset,
            second_read_next_offset,
            expected_len,
        ) = task.await.expect("join multi-client runtime task");
        max_commit_index = max_commit_index.max(second_commit_index);
        trace.push(SimEvent::RuntimeMultiClientAppendCommitted {
            stream: stream.clone(),
            client_id,
            append_index: 0,
            start_offset: first_start_offset,
            next_offset: first_next_offset,
        });
        trace.push(SimEvent::RuntimeMultiClientReadVerified {
            stream: stream.clone(),
            client_id,
            expected_len: usize::try_from(first_read_next_offset).expect("offset fits usize"),
            next_offset: first_read_next_offset,
        });
        trace.push(SimEvent::RuntimeMultiClientAppendCommitted {
            stream: stream.clone(),
            client_id,
            append_index: 1,
            start_offset: second_start_offset,
            next_offset: second_next_offset,
        });
        trace.push(SimEvent::RuntimeMultiClientReadVerified {
            stream,
            client_id,
            expected_len,
            next_offset: second_read_next_offset,
        });
    }
    trace.push(SimEvent::RuntimeMultiClientVerified {
        stream_count: 4,
        total_appends: 8,
    });

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: max_commit_index,
        trace,
    }
}

pub(super) async fn run_runtime_cold_flush_worker_inner(
    config: ThreeNodeRaftSimConfig,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let cold_store = Arc::new(sim_cold_store());
    let mut runtime_config = RuntimeConfig::new(2, 4);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    runtime_config.mailbox_capacity = 4;
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn hosted runtime actors with simulated cold store");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::RuntimeColdFlushActorsBuilt {
        stream_count: 2,
        core_count: 2,
        raft_group_count: 4,
    });

    let streams = choose_runtime_streams_spanning_placement(&runtime, &config.stream, 2);
    let mut cores = BTreeSet::new();
    let mut groups = BTreeSet::new();
    let mut max_commit_index = 0;
    for stream in &streams {
        let placement = runtime.locate(stream);
        cores.insert(placement.core_id.0);
        groups.insert(placement.raft_group_id.0);
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create cold flush stream through hosted runtime actors");
        trace.push(SimEvent::RuntimeColdFlushStreamCreated {
            stream: stream.clone(),
            core_id: placement.core_id.0,
            raft_group_id: placement.raft_group_id.0,
        });
        let append = runtime
            .append(AppendRequest::from_bytes(
                stream.clone(),
                b"abcdef".to_vec(),
            ))
            .await
            .expect("append cold flush stream through hosted runtime actors");
        max_commit_index = max_commit_index.max(append.group_commit_index);
    }
    assert!(
        cores.len() >= 2,
        "runtime cold flush scenario should span at least two cores"
    );
    assert!(
        groups.len() >= 2,
        "runtime cold flush scenario should span at least two raft groups"
    );

    let flushed = runtime
        .flush_cold_all_groups_once_bounded(
            PlanGroupColdFlushRequest {
                min_hot_bytes: 4,
                max_flush_bytes: 4,
            },
            2,
        )
        .await
        .expect("flush cold chunks through hosted runtime actor API");
    assert_eq!(flushed, streams.len());
    let metrics = runtime.metrics().snapshot();
    assert_eq!(metrics.cold_flush_uploads, streams.len() as u64);
    assert_eq!(metrics.cold_flush_upload_bytes, 4 * streams.len() as u64);
    assert_eq!(metrics.cold_flush_publishes, streams.len() as u64);
    assert_eq!(metrics.cold_flush_publish_bytes, 4 * streams.len() as u64);
    trace.push(SimEvent::RuntimeColdFlushCompleted {
        flushed_count: flushed,
        upload_count: metrics.cold_flush_uploads,
        publish_count: metrics.cold_flush_publishes,
    });

    for stream in streams {
        let read = runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 6,
                now_ms: 0,
            })
            .await
            .expect("read cold and hot payload through hosted runtime actors");
        assert_eq!(read.payload, b"abcdef");
        assert_eq!(read.next_offset, 6);
        trace.push(SimEvent::RuntimeColdLiveReadVerified {
            stream,
            next_offset: read.next_offset,
        });
    }

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: max_commit_index,
        trace,
    }
}

pub(super) async fn run_runtime_seeded_interleaving_inner(
    config: ThreeNodeRaftSimConfig,
    plan: RuntimeInterleavingPlan,
) -> ThreeNodeRaftSimOutcome {
    let mut trace = SimTrace::default();
    let cold_store = Arc::new(sim_cold_store());
    let mut runtime_config = RuntimeConfig::new(2, 4);
    runtime_config.threading = RuntimeThreading::HostedTokio;
    runtime_config.mailbox_capacity = 4;
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        runtime_config,
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store.clone()),
    )
    .expect("spawn hosted runtime actors with simulated cold store");
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    trace.push(SimEvent::RuntimeInterleavingActorsBuilt {
        client_count: plan.clients.len(),
        stream_count: plan.clients.len(),
        core_count: 2,
        raft_group_count: 4,
    });
    trace.push(SimEvent::RuntimeInterleavingPlanSelected {
        flush_delay_ms: plan.flush_delay_ms,
        read_verify_delay_ms: plan.read_verify_delay_ms,
    });

    let streams =
        choose_runtime_streams_spanning_placement(&runtime, &config.stream, plan.clients.len());
    let mut cores = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for client in &plan.clients {
        let stream = streams
            .get(client.stream_index)
            .unwrap_or_else(|| panic!("missing stream for client {}", client.client_id));
        let placement = runtime.locate(stream);
        cores.insert(placement.core_id.0);
        groups.insert(placement.raft_group_id.0);
        runtime
            .create_stream(CreateStreamRequest::new(
                stream.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create seeded interleaving stream through hosted runtime actors");
        trace.push(SimEvent::RuntimeInterleavingClientPlanned {
            client_id: client.client_id,
            stream: stream.clone(),
            first_append_delay_ms: client.first_append_delay_ms,
            second_append_delay_ms: client.second_append_delay_ms,
            core_id: placement.core_id.0,
            raft_group_id: placement.raft_group_id.0,
        });
    }
    assert!(
        plan.clients.len() < 2 || cores.len() >= 2,
        "runtime interleaving scenario should span at least two cores"
    );
    assert!(
        plan.clients.len() < 2 || groups.len() >= 2,
        "runtime interleaving scenario should span at least two raft groups"
    );

    let flush_runtime = runtime.clone();
    let flush_delay_ms = plan.flush_delay_ms;
    let flush_group_limit = plan.flush_group_limit;
    let flush_task = madsim::task::spawn(async move {
        madsim::time::sleep(Duration::from_millis(flush_delay_ms)).await;
        let flushed = flush_runtime
            .flush_cold_all_groups_once_bounded(
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 4,
                    max_flush_bytes: 4,
                },
                flush_group_limit,
            )
            .await
            .expect("seeded interleaving cold flush");
        let metrics = flush_runtime.metrics().snapshot();
        (
            flushed,
            metrics.cold_flush_uploads,
            metrics.cold_flush_publishes,
        )
    });

    let mut tasks = Vec::new();
    for client in plan.clients.iter().cloned() {
        let runtime = runtime.clone();
        let stream = streams[client.stream_index].clone();
        tasks.push(madsim::task::spawn(async move {
            let first_payload = runtime_interleaving_payload(client.client_id, 0);
            let second_payload = runtime_interleaving_payload(client.client_id, 1);
            madsim::time::sleep(Duration::from_millis(client.first_append_delay_ms)).await;
            let first = runtime
                .append(AppendRequest::from_bytes(
                    stream.clone(),
                    first_payload.clone(),
                ))
                .await
                .expect("first seeded interleaving append");
            madsim::time::sleep(Duration::from_millis(client.second_append_delay_ms)).await;
            let second = runtime
                .append(AppendRequest::from_bytes(
                    stream.clone(),
                    second_payload.clone(),
                ))
                .await
                .expect("second seeded interleaving append");
            let mut expected = first_payload;
            expected.extend_from_slice(&second_payload);
            (
                stream,
                client.client_id,
                first.start_offset,
                first.next_offset,
                second.start_offset,
                second.next_offset,
                second.group_commit_index,
                expected,
            )
        }));
    }

    let (flushed, upload_count, publish_count) = flush_task
        .await
        .expect("join seeded interleaving flush task");
    assert_eq!(upload_count, flushed as u64);
    assert_eq!(publish_count, flushed as u64);
    trace.push(SimEvent::RuntimeInterleavingFlushCompleted {
        flushed_count: flushed,
        upload_count,
        publish_count,
    });
    maybe_panic_after_runtime_interleaving_event(&plan, "runtime_interleaving_flush_completed");

    let mut max_commit_index = 0;
    let mut completed = Vec::new();
    for task in tasks {
        let (
            stream,
            client_id,
            first_start_offset,
            first_next_offset,
            second_start_offset,
            second_next_offset,
            second_commit_index,
            expected,
        ) = task.await.expect("join seeded interleaving client task");
        max_commit_index = max_commit_index.max(second_commit_index);
        trace.push(SimEvent::RuntimeInterleavingAppendCommitted {
            client_id,
            append_index: 0,
            stream: stream.clone(),
            start_offset: first_start_offset,
            next_offset: first_next_offset,
        });
        trace.push(SimEvent::RuntimeInterleavingAppendCommitted {
            client_id,
            append_index: 1,
            stream: stream.clone(),
            start_offset: second_start_offset,
            next_offset: second_next_offset,
        });
        completed.push((stream, client_id, expected));
    }

    let cold_read_fault_consumed = if plan.runtime_cold_write_failure.is_some()
        || plan.runtime_cold_read_delay_ms.is_some()
        || plan.runtime_cold_read_truncate_len.is_some()
    {
        let write_failure = plan.runtime_cold_write_failure.clone();
        let write_fault_consumed = Arc::new(Mutex::new(false));
        if let Some(message) = write_failure.clone() {
            let consumed_for_policy = Arc::clone(&write_fault_consumed);
            cold_store.set_fault_policy(move |context| {
                if context.operation != ColdStoreOperation::WriteChunk {
                    return None;
                }
                let mut consumed = consumed_for_policy
                    .lock()
                    .expect("runtime cold write fault policy mutex");
                if *consumed {
                    return None;
                }
                *consumed = true;
                Some(ColdStoreFaultEffect::fail(message.clone()))
            });
        }

        let final_flushed = match runtime
            .flush_cold_all_groups_once_bounded(
                PlanGroupColdFlushRequest {
                    min_hot_bytes: 4,
                    max_flush_bytes: 4,
                },
                4,
            )
            .await
        {
            Ok(flushed) => flushed,
            Err(err) => {
                if write_failure.is_some() {
                    assert!(
                        *write_fault_consumed
                            .lock()
                            .expect("runtime cold write fault consumed mutex"),
                        "runtime interleaving cold write fault should be consumed"
                    );
                    let message = format!("final cold flush failed: {err}");
                    SimTrace::record(SimEvent::InvariantFailed {
                        invariant: "runtime_interleaving_cold_write_integrity".to_owned(),
                        after_event: "runtime_interleaving_flush_completed".to_owned(),
                        message: message.clone(),
                    });
                    panic!(
                        "invariant `runtime_interleaving_cold_write_integrity` failed after `runtime_interleaving_flush_completed`: {message}"
                    );
                }
                panic!("final seeded interleaving cold flush before cold read fault: {err}");
            }
        };
        let metrics = runtime.metrics().snapshot();
        trace.push(SimEvent::RuntimeInterleavingFlushCompleted {
            flushed_count: final_flushed,
            upload_count: metrics.cold_flush_uploads,
            publish_count: metrics.cold_flush_publishes,
        });
        let delay_ms = plan.runtime_cold_read_delay_ms;
        let truncate_len = plan.runtime_cold_read_truncate_len;
        let consumed = Arc::new(Mutex::new(false));
        let consumed_for_policy = Arc::clone(&consumed);
        cold_store.set_fault_policy(move |context| {
            if context.operation != ColdStoreOperation::ReadObjectRange {
                return None;
            }
            let mut consumed = consumed_for_policy
                .lock()
                .expect("runtime cold read delay policy mutex");
            if *consumed {
                return None;
            }
            *consumed = true;
            Some(ColdStoreFaultEffect {
                delay: delay_ms.map(Duration::from_millis),
                error: None,
                truncate_read_to: truncate_len,
            })
        });
        Some((delay_ms, truncate_len, consumed))
    } else {
        None
    };

    madsim::time::sleep(Duration::from_millis(plan.read_verify_delay_ms)).await;
    let read_started = madsim::time::Instant::now();
    completed.sort_by_key(|(_, client_id, _)| *client_id);
    for (stream, client_id, mut expected) in completed {
        if plan.corrupt_read_client_id == Some(client_id) {
            expected.push(b'!');
        }
        let read = match runtime
            .read_stream(ReadStreamRequest {
                stream_id: stream.clone(),
                offset: 0,
                max_len: 32,
                now_ms: 0,
            })
            .await
        {
            Ok(read) => read,
            Err(err) => {
                let message = format!("client {client_id} stream {stream} read failed: {err}");
                SimTrace::record(SimEvent::InvariantFailed {
                    invariant: "runtime_interleaving_cold_read_integrity".to_owned(),
                    after_event: "runtime_interleaving_read_verified".to_owned(),
                    message: message.clone(),
                });
                panic!(
                    "invariant `runtime_interleaving_cold_read_integrity` failed after `runtime_interleaving_read_verified`: {message}"
                );
            }
        };
        assert_runtime_interleaving_read_your_write(
            client_id,
            &stream,
            &read.payload,
            &expected,
            "runtime_interleaving_read_verified",
        );
        trace.push(SimEvent::RuntimeInterleavingReadVerified {
            client_id,
            stream,
            expected_len: expected.len(),
            next_offset: read.next_offset,
        });
    }
    if let Some((delay_ms, _truncate_len, consumed)) = cold_read_fault_consumed {
        assert!(
            *consumed
                .lock()
                .expect("runtime cold read fault consumed mutex"),
            "runtime interleaving cold read fault should be consumed"
        );
        if let Some(delay_ms) = delay_ms {
            assert!(
                read_started.elapsed() >= Duration::from_millis(delay_ms),
                "runtime interleaving cold read should observe at least the injected virtual delay"
            );
            trace.push(SimEvent::RuntimeInterleavingColdReadDelayVerified { delay_ms });
        }
    }
    trace.push(SimEvent::RuntimeInterleavingVerified {
        client_count: plan.clients.len(),
        total_appends: plan.clients.len() * 2,
    });
    maybe_panic_after_runtime_interleaving_event(&plan, "runtime_interleaving_verified");

    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: 0,
        target_node_id: None,
        appended_log_index: max_commit_index,
        trace,
    }
}
