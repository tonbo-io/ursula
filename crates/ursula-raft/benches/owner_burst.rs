//! Single-node OpenRaft burst versus serial appends with fsync. Each iteration
//! resets its streams outside the timer, keeping retained state bounded.
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use futures_util::future::join_all;
use ursula_runtime::AppendRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;

fn owner_burst(c: &mut Criterion) {
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime");
    let mut group = c.benchmark_group("owner_wal_burst");
    group.sample_size(10);
    group.throughput(Throughput::Elements(32));
    for cores in [1, 4] {
        let root = tempfile::tempdir().expect("WAL directory");
        let wal = ursula_raft::wal::RaftWal::start(
            root.path(),
            ursula_config::WalFsync::Always,
            &ursula_shard::StaticShardMap::new(cores, cores).expect("topology"),
        )
        .expect("WAL");
        let registry = ursula_raft::RaftGroupHandleRegistry::default();
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig::new(cores, cores),
            ursula_raft::DurableRaftGroupEngineFactory::new(wal).with_registry(registry.clone()),
        )
        .expect("owner workers");
        let streams = (0..cores)
            .map(|core| {
                (0..10000)
                    .map(|i| BucketStreamId::new("benchcmp", format!("burst-{i}")))
                    .find(|stream| usize::from(runtime.locate(stream).core_id.0) == core)
                    .expect("stream on core")
            })
            .collect::<Vec<_>>();
        caller.block_on(async {
            for stream in &streams {
                runtime
                    .create_stream(CreateStreamRequest::new(
                        stream.clone(),
                        "application/octet-stream",
                    ))
                    .await
                    .expect("create stream");
            }
        });
        for burst in [false, true] {
            group.bench_with_input(
                BenchmarkId::new(if burst { "burst" } else { "serial" }, cores),
                &cores,
                |b, _| {
                    b.iter_batched(
                        || {
                            caller.block_on(async {
                                for stream in &streams {
                                    runtime
                                        .delete_stream(DeleteStreamRequest {
                                            stream_id: stream.clone(),
                                            if_incarnation: None,
                                        })
                                        .await
                                        .expect("reset stream");
                                    runtime
                                        .create_stream(CreateStreamRequest::new(
                                            stream.clone(),
                                            "application/octet-stream",
                                        ))
                                        .await
                                        .expect("create stream");
                                }
                            });
                            streams
                                .iter()
                                .cycle()
                                .take(32)
                                .map(|stream| {
                                    AppendRequest::from_bytes(stream.clone(), vec![7; 256])
                                })
                                .collect::<Vec<_>>()
                        },
                        |requests| {
                            let replies = caller.block_on(async {
                                let replies = if burst {
                                    join_all(
                                        requests.into_iter().map(|request| runtime.append(request)),
                                    )
                                    .await
                                } else {
                                    let mut replies = Vec::new();
                                    for request in requests {
                                        replies.push(runtime.append(request).await);
                                    }
                                    replies
                                };
                                // Exercise the same owner endpoint that external Raft calls use.
                                for group in 0..cores {
                                    registry
                                        .get(ursula_shard::RaftGroupId(
                                            u32::try_from(group).expect("group"),
                                        ))
                                        .expect("registered")
                                        .with_raft_state(|_state| ())
                                        .await
                                        .expect("owner observation");
                                }
                                replies
                            });
                            for reply in replies {
                                criterion::black_box(reply.expect("append"));
                            }
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
        caller.block_on(async {
            runtime.stop_owner_services().await;
            runtime.shutdown_group_engines().await.expect("stop groups");
            assert!(runtime.shutdown_owners().await.is_empty());
        });
    }
    group.finish();
}
criterion_group!(benches, owner_burst);
criterion_main!(benches);
