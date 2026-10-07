//! Loopback HTTP + owner mailbox cost, excluding Raft/storage and client DNS.
use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeThreading;
use ursula_runtime::ShardRuntime;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;

fn http_ownership(c: &mut Criterion) {
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("client runtime");
    let mut group = c.benchmark_group("http_owner_placement");
    for on_owner in [false, true] {
        let mut config = RuntimeConfig::new(1, 1);
        config.threading = RuntimeThreading::ThreadPerCore;
        let runtime = executor.block_on(async { ShardRuntime::spawn(config).expect("owner") });
        executor.block_on(runtime.warm_all_groups()).expect("warm");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let url = format!("http://{}/", listener.local_addr().expect("address"));
        let route_runtime = runtime.clone();
        let app = axum::Router::new().route(
            "/",
            axum::routing::get(move || {
                let runtime = route_runtime.clone();
                async move {
                    black_box(runtime.state_gauges(RaftGroupId(0)).await.expect("gauges"));
                    "ok"
                }
            }),
        );
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let serve = async move {
            ursula_observability::serve::serve_until_shutdown(
                tokio::net::TcpListener::from_std(listener).expect("reactor"),
                app,
                async {
                    let _closed = stop_rx.await;
                },
                None,
            )
            .await
        };
        let mut main_task = None;
        let done = if on_owner {
            runtime.spawn_on_owner(CoreId(0), serve).expect("schedule")
        } else {
            let (tx, rx) = tokio::sync::oneshot::channel();
            main_task = Some(executor.spawn(async move {
                tx.send(serve.await).expect("completion receiver");
            }));
            rx
        };
        let client = reqwest::Client::new();
        group.bench_function(if on_owner { "owner" } else { "main_runtime" }, |b| {
            b.iter(|| {
                executor.block_on(async {
                    let requests = (0..32).map(|_| async {
                        let body = client
                            .get(&url)
                            .send()
                            .await
                            .expect("response")
                            .bytes()
                            .await
                            .expect("body");
                        black_box(body);
                    });
                    futures_util::future::join_all(requests).await;
                })
            });
        });
        stop_tx.send(()).expect("stop");
        executor
            .block_on(done)
            .expect("server completion")
            .expect("drain");
        if let Some(task) = main_task {
            executor.block_on(task).expect("main task");
        }
        executor.block_on(runtime.shutdown_owners());
    }
    group.finish();
}
criterion_group!(benches, http_ownership);
criterion_main!(benches);
