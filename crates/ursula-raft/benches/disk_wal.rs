use std::sync::Arc;

use bytes::Bytes;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use futures_util::future::try_join_all;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::vote::RaftLeaderId;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use tempfile::TempDir;
use ursula_config::WalFsync;
use ursula_raft::DurableRaftLogStoreFactory;
use ursula_raft::RaftGroupFileLogStore;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

mod support;

use support::raft_log_store::BenchmarkRaftLogStore;

const APPENDS_PER_ITER: usize = 16;

#[derive(Clone, Copy, Debug)]
enum Backend {
    SharedPerCore(WalFsync),
    UpstreamRaftLog,
}

impl Backend {
    const ALL: [Self; 3] = [
        Self::SharedPerCore(WalFsync::Always),
        Self::SharedPerCore(WalFsync::Never),
        Self::UpstreamRaftLog,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::SharedPerCore(WalFsync::Always) => "shared-per-core-always",
            Self::SharedPerCore(WalFsync::Never) => "shared-per-core-never",
            Self::UpstreamRaftLog => "upstream-raft-log",
        }
    }
}

#[derive(Clone)]
enum BenchStore {
    Ursula(Arc<RaftGroupFileLogStore>),
    Upstream(BenchmarkRaftLogStore<UrsulaRaftTypeConfig>),
}

struct Stores {
    _dir: TempDir,
    stores: Vec<BenchStore>,
}

fn disk_wal_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build benchmark runtime");
    let full = std::env::var_os("URSULA_WAL_BENCH_FULL").is_some();
    let group_counts: &[usize] = if full { &[1, 16, 256] } else { &[1, 16] };
    let payload_sizes: &[usize] = if full {
        &[256, 4 * 1024, 64 * 1024]
    } else {
        &[256, 4 * 1024]
    };

    let mut append = c.benchmark_group("disk_wal_append_durable");
    append.sample_size(if full { 20 } else { 10 });
    append.throughput(Throughput::Elements(
        u64::try_from(APPENDS_PER_ITER).expect("append count fits u64"),
    ));
    for &group_count in group_counts {
        for &payload_size in payload_sizes {
            for backend in Backend::ALL {
                let id = BenchmarkId::new(
                    backend.name(),
                    format!("groups={group_count}/payload={payload_size}"),
                );
                append.bench_with_input(id, &(backend, group_count, payload_size), |b, input| {
                    b.to_async(&runtime).iter_batched(
                        || setup_stores(input.0, input.1),
                        |stores| append_waves(stores, input.2, APPENDS_PER_ITER, false),
                        BatchSize::LargeInput,
                    );
                });
            }
        }
    }
    append.finish();

    let mut append_committed = c.benchmark_group("disk_wal_append_and_commit");
    append_committed.sample_size(if full { 20 } else { 10 });
    append_committed.throughput(Throughput::Elements(
        u64::try_from(APPENDS_PER_ITER).expect("append count fits u64"),
    ));
    for &group_count in group_counts {
        for backend in Backend::ALL {
            append_committed.bench_with_input(
                BenchmarkId::new(backend.name(), format!("groups={group_count}")),
                &(backend, group_count),
                |b, input| {
                    b.to_async(&runtime).iter_batched(
                        || setup_stores(input.0, input.1),
                        |stores| append_waves(stores, 256, APPENDS_PER_ITER, true),
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    append_committed.finish();

    let mut recovery = c.benchmark_group("disk_wal_recovery");
    recovery.sample_size(if full { 20 } else { 10 });
    for historical_entries in if full { [1_024, 16_384] } else { [256, 1_024] } {
        let dir = runtime.block_on(prepare_recovery_journal(historical_entries, 256));
        recovery.bench_with_input(
            BenchmarkId::from_parameter(historical_entries),
            &historical_entries,
            |b, _| {
                // Times the journal's recovery only. Each run starts and
                // shuts down cleanly outside the timing, so every recovery
                // reads the journal strictly.
                b.iter_custom(|iters| {
                    let mut elapsed = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let factory =
                            DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always)
                                .expect("start the benchmark WAL");
                        let started_at = std::time::Instant::now();
                        let reopened = factory
                            .open(
                                placement(0),
                                RuntimeMetrics::new(1, 1).group_engine_metrics(),
                            )
                            .expect("open shared-core benchmark WAL");
                        elapsed = elapsed.saturating_add(started_at.elapsed());
                        black_box(&reopened);
                        drop(reopened);
                        runtime
                            .block_on(factory.shutdown())
                            .expect("shut down the benchmark WAL");
                    }
                    elapsed
                });
            },
        );
        black_box(dir);
    }
    recovery.finish();

    let mut reads = c.benchmark_group("disk_wal_recent_read");
    reads.sample_size(if full { 20 } else { 10 });
    reads.bench_function("1024-entries/last-64", |b| {
        let (_dir, mut store) = runtime.block_on(prepare_read_store(1_024, 256));
        b.to_async(&runtime).iter(|| {
            let mut reader = store.clone();
            async move {
                let entries = reader
                    .try_get_log_entries(961..1_025)
                    .await
                    .expect("read benchmark WAL");
                black_box(entries);
            }
        });
        black_box(&mut store);
    });
    reads.finish();
}

/// Opens `group_count` stores on a new journal, each holding one entry, so
/// the timed appends start at index 2 and measure steady-state appends. A
/// group's first entry also replaces the core's metadata file (its
/// `initialized` flag), a one-time cost per group.
fn setup_stores(backend: Backend, group_count: usize) -> Stores {
    let dir = tempfile::tempdir().expect("create WAL benchmark directory");
    let group_count_u32 = u32::try_from(group_count).expect("benchmark group count fits u32");
    let stores: Vec<BenchStore> = match backend {
        Backend::SharedPerCore(fsync) => {
            let metrics = RuntimeMetrics::new(1, group_count);
            let factory = DurableRaftLogStoreFactory::start(dir.path(), fsync)
                .expect("start the benchmark WAL");
            (0..group_count_u32)
                .map(|group_id| {
                    factory
                        .open(placement(group_id), metrics.group_engine_metrics())
                        .expect("open shared-core benchmark WAL")
                        .into()
                })
                .collect()
        }
        Backend::UpstreamRaftLog => (0..group_count_u32)
            .map(|group_id| {
                BenchmarkRaftLogStore::open(
                    dir.path()
                        .join(format!("raft-log-{group_id}"))
                        .display()
                        .to_string(),
                )
                .expect("open upstream raft-log benchmark WAL")
                .into()
            })
            .collect(),
    };
    warm_up(&stores);
    Stores { _dir: dir, stores }
}

/// Appends entry 1 to every store, outside the timed routine. Criterion runs
/// setup inside the benchmark runtime, so the warm-up runs on a thread of
/// its own.
fn warm_up(stores: &[BenchStore]) {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("build warm-up runtime")
                    .block_on(try_join_all(stores.iter().enumerate().map(
                        |(group_index, store)| {
                            let mut store = store.clone();
                            let group_id = u32::try_from(group_index).expect("group fits u32");
                            async move {
                                let entry = entry(1, group_id, 256);
                                match &mut store {
                                    BenchStore::Ursula(store) => {
                                        store.append([entry], IOFlushed::noop()).await
                                    }
                                    BenchStore::Upstream(store) => {
                                        store.append_durable(vec![entry]).await
                                    }
                                }
                            }
                        },
                    )))
                    .expect("warm up the benchmark WAL");
            })
            .join()
            .expect("warm-up thread");
    });
}

/// Returns the stores, so criterion drops them (and the writers `fsync` on
/// their way out) outside the timed routine.
async fn append_waves(
    stores: Stores,
    payload_size: usize,
    append_count: usize,
    save_committed: bool,
) -> Stores {
    let mut next_indexes = vec![2_u64; stores.stores.len()];
    let mut remaining = append_count;
    while remaining > 0 {
        let wave = remaining.min(stores.stores.len());
        let writes = stores
            .stores
            .iter()
            .take(wave)
            .enumerate()
            .map(|(group_index, store)| {
                let mut store = store.clone();
                let slot = next_indexes
                    .get_mut(group_index)
                    .expect("one next index per store in the wave");
                let index = *slot;
                *slot = index.saturating_add(1);
                async move {
                    let entry = entry(
                        index,
                        u32::try_from(group_index).expect("group index fits u32"),
                        payload_size,
                    );
                    match &mut store {
                        BenchStore::Ursula(store) => {
                            store.append([entry], IOFlushed::noop()).await?;
                            if save_committed {
                                store.save_committed(Some(log_id(index))).await?;
                            }
                        }
                        BenchStore::Upstream(store) => {
                            store.append_durable(vec![entry]).await?;
                            if save_committed {
                                store.save_committed(Some(log_id(index))).await?;
                            }
                        }
                    }
                    Ok::<(), std::io::Error>(())
                }
            });
        try_join_all(writes).await.expect("append benchmark wave");
        remaining = remaining.saturating_sub(wave);
    }
    stores
}

impl From<Arc<RaftGroupFileLogStore>> for BenchStore {
    fn from(store: Arc<RaftGroupFileLogStore>) -> Self {
        Self::Ursula(store)
    }
}

impl From<BenchmarkRaftLogStore<UrsulaRaftTypeConfig>> for BenchStore {
    fn from(store: BenchmarkRaftLogStore<UrsulaRaftTypeConfig>) -> Self {
        Self::Upstream(store)
    }
}

/// Opens one group's store on the shared per-core journal under `root`.
fn open_shared_store(root: &std::path::Path, group_id: u32) -> Arc<RaftGroupFileLogStore> {
    DurableRaftLogStoreFactory::start(root, WalFsync::Always)
        .expect("start the benchmark WAL")
        .open(
            placement(group_id),
            RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("open shared-core benchmark WAL")
}

async fn prepare_recovery_journal(entries: usize, payload_size: usize) -> TempDir {
    let dir = tempfile::tempdir().expect("create recovery benchmark directory");
    let factory = DurableRaftLogStoreFactory::start(dir.path(), WalFsync::Always)
        .expect("start the benchmark WAL");
    let mut store = factory
        .open(
            placement(0),
            RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("open shared-core benchmark WAL");
    let batch = (1..=entries)
        .map(|index| {
            entry(
                u64::try_from(index).expect("entry index fits u64"),
                0,
                payload_size,
            )
        })
        .collect::<Vec<_>>();
    store
        .append(batch, IOFlushed::noop())
        .await
        .expect("prepare recovery benchmark WAL");
    drop(store);
    factory
        .shutdown()
        .await
        .expect("shut down the benchmark WAL");
    dir
}

async fn prepare_read_store(
    entries: usize,
    payload_size: usize,
) -> (TempDir, Arc<RaftGroupFileLogStore>) {
    let dir = tempfile::tempdir().expect("create read benchmark directory");
    let mut store = open_shared_store(dir.path(), 0);
    let batch = (1..=entries)
        .map(|index| {
            entry(
                u64::try_from(index).expect("entry index fits u64"),
                0,
                payload_size,
            )
        })
        .collect::<Vec<_>>();
    store
        .append(batch, IOFlushed::noop())
        .await
        .expect("prepare read benchmark WAL");
    (dir, store)
}

fn placement(group_id: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(group_id),
        raft_group_id: RaftGroupId(group_id),
    }
}

fn log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
    LogId {
        leader_id: CommittedLeaderId::new(1, 1),
        index,
    }
}

fn entry(index: u64, group_id: u32, payload_size: usize) -> EntryOf<UrsulaRaftTypeConfig> {
    EntryOf::<UrsulaRaftTypeConfig>::new(
        log_id(index),
        EntryPayload::Normal(GroupWriteCommand::Stream(StreamCommand::Append {
            stream_id: BucketStreamId::new("wal-bench", format!("stream-{group_id}")),
            content_type: Some("application/octet-stream".to_owned()),
            payload: Bytes::from(vec![7_u8; payload_size]),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })),
    )
}

criterion_group!(benches, disk_wal_benches);
criterion_main!(benches);
