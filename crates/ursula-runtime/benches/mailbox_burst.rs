//! Request-dispatch baseline, excluding Raft and WAL latency. Each iteration
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

fn mailbox_burst(c: &mut Criterion) {
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime");
    let mut group = c.benchmark_group("mailbox_burst");
    group.throughput(Throughput::Elements(32));
    for cores in [1, 4] {
        let runtime = ShardRuntime::spawn(RuntimeConfig::new(cores, 64)).expect("owner workers");
        let streams = (0..32)
            .map(|i| BucketStreamId::new("benchcmp", format!("burst-{i}")))
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
        group.bench_with_input(BenchmarkId::new("owner_cores", cores), &cores, |b, _| {
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
                        .map(|stream| AppendRequest::from_bytes(stream.clone(), vec![7; 256]))
                        .collect::<Vec<_>>()
                },
                |requests| {
                    let replies = caller.block_on(join_all(
                        requests.into_iter().map(|request| runtime.append(request)),
                    ));
                    for reply in replies {
                        criterion::black_box(reply.expect("append"));
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}
criterion_group!(benches, mailbox_burst);
criterion_main!(benches);
