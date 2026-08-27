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
    DirectPerGroup,
    SharedPerCore,
    UpstreamRaftLog,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Self::DirectPerGroup => "direct-per-group",
            Self::SharedPerCore => "shared-per-core",
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
            for backend in [
                Backend::DirectPerGroup,
                Backend::SharedPerCore,
                Backend::UpstreamRaftLog,
            ] {
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
        for backend in [
            Backend::DirectPerGroup,
            Backend::SharedPerCore,
            Backend::UpstreamRaftLog,
        ] {
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
        let (dir, path) = runtime.block_on(prepare_direct_store(historical_entries, 256));
        recovery.bench_with_input(
            BenchmarkId::from_parameter(historical_entries),
            &historical_entries,
            |b, _| {
                b.iter(|| {
                    let reopened =
                        RaftGroupFileLogStore::shared(&path).expect("reopen benchmark WAL");
                    black_box(reopened);
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

fn setup_stores(backend: Backend, group_count: usize) -> Stores {
    let dir = tempfile::tempdir().expect("create WAL benchmark directory");
    let group_count_u32 = u32::try_from(group_count).expect("benchmark group count fits u32");
    let stores = match backend {
        Backend::DirectPerGroup => (0..group_count_u32)
            .map(|group_id| {
                RaftGroupFileLogStore::shared(dir.path().join(format!("group-{group_id}.wal")))
                    .expect("open direct benchmark WAL")
                    .into()
            })
            .collect(),
        Backend::SharedPerCore => {
            let metrics = RuntimeMetrics::new(1, group_count);
            let factory = DurableRaftLogStoreFactory::new(dir.path());
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
    Stores { _dir: dir, stores }
}

async fn append_waves(
    stores: Stores,
    payload_size: usize,
    append_count: usize,
    save_committed: bool,
) {
    let mut next_indexes = vec![1_u64; stores.stores.len()];
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
                let index = next_indexes[group_index];
                next_indexes[group_index] = index.saturating_add(1);
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
        remaining -= wave;
    }
    black_box(stores);
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

async fn prepare_direct_store(
    entries: usize,
    payload_size: usize,
) -> (TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("create recovery benchmark directory");
    let path = dir.path().join("group.wal");
    {
        let mut store = RaftGroupFileLogStore::shared(&path).expect("open recovery benchmark WAL");
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
    }
    (dir, path)
}

async fn prepare_read_store(
    entries: usize,
    payload_size: usize,
) -> (TempDir, Arc<RaftGroupFileLogStore>) {
    let dir = tempfile::tempdir().expect("create read benchmark directory");
    let mut store = RaftGroupFileLogStore::shared(dir.path().join("group.wal"))
        .expect("open read benchmark WAL");
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
            stream_id: BucketStreamId::with_affinity("wal-bench", group_id.to_string(), "stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: Bytes::from(vec![7_u8; payload_size]),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        })),
    )
}

criterion_group!(benches, disk_wal_benches);
criterion_main!(benches);
