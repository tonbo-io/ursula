// Stresses the production ThreadPerCore runtime; cfg(not(madsim))-only by
// design (DoD #1). Under cfg(madsim) the bin is a no-op.

#[cfg(madsim)]
fn main() {}

#[cfg(not(madsim))]
use std::path::PathBuf;
#[cfg(not(madsim))]
use std::sync::Arc;
#[cfg(not(madsim))]
use std::sync::atomic::AtomicU64;
#[cfg(not(madsim))]
use std::sync::atomic::Ordering;
#[cfg(not(madsim))]
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::Instant;

#[cfg(not(madsim))]
use tokio::task::JoinSet;
#[cfg(not(madsim))]
use ursula_config::WalFsync;
#[cfg(not(madsim))]
use ursula_raft::DurableRaftGroupEngineFactory;
#[cfg(not(madsim))]
use ursula_raft::DurableRaftLogStoreFactory;
#[cfg(not(madsim))]
use ursula_runtime::AppendRequest;
#[cfg(not(madsim))]
use ursula_runtime::CreateStreamRequest;
#[cfg(not(madsim))]
use ursula_runtime::RuntimeConfig;
#[cfg(not(madsim))]
use ursula_runtime::RuntimeThreading;
#[cfg(not(madsim))]
use ursula_runtime::ShardRuntime;
#[cfg(not(madsim))]
use ursula_shard::BucketStreamId;

#[cfg(not(madsim))]
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

#[cfg(not(madsim))]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse()?;
    let mut config = RuntimeConfig::new(args.core_count, args.raft_group_count);
    config.mailbox_capacity = args.mailbox_capacity;
    config.threading = RuntimeThreading::ThreadPerCore;
    // Without --wal-dir the journals go to a fresh directory removed at exit.
    let (wal_dir, remove_wal_dir) = match &args.wal_dir {
        Some(dir) => (dir.clone(), false),
        None => (
            std::env::temp_dir().join(format!("ursula-raft-stress-{}", std::process::id())),
            true,
        ),
    };
    let wal = DurableRaftLogStoreFactory::start(&wal_dir, args.wal_fsync)?;
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        DurableRaftGroupEngineFactory::new(wal.clone()),
    )?;

    let streams = (0..args.stream_count)
        .map(|index| BucketStreamId::new("stress", format!("stream-{index}")))
        .collect::<Vec<_>>();
    create_streams(&runtime, &streams, args.setup_concurrency).await?;

    let total_appends = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now()
        .checked_add(args.duration)
        .ok_or("--duration-secs is too large")?;
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for producer_index in 0..args.producer_count {
        let runtime = runtime.clone();
        let streams = streams.clone();
        let total_appends = total_appends.clone();
        let args = args.clone();
        tasks.spawn(async move {
            let payload = vec![0; args.payload_bytes];
            let stream_len = streams.len();
            let mut stream_index = producer_index
                .checked_rem(stream_len)
                .ok_or("stream list is empty")?;
            while Instant::now() < deadline {
                let stream = streams
                    .get(stream_index)
                    .expect("stream_index wraps below streams.len()")
                    .clone();
                stream_index = stream_index
                    .checked_add(args.producer_count)
                    .and_then(|next| next.checked_rem(stream_len))
                    .ok_or("stream index overflow")?;

                let mut request = AppendRequest::from_bytes(stream, payload.clone());
                request.content_type = DEFAULT_CONTENT_TYPE.to_owned();
                runtime.append(request).await?;
                total_appends.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let snapshot = runtime.metrics().snapshot();
    let active_cores = snapshot
        .per_core_appends
        .iter()
        .filter(|value| **value > 0)
        .count();
    let active_groups = snapshot
        .per_group_appends
        .iter()
        .filter(|value| **value > 0)
        .count();
    let counted_appends = total_appends.load(Ordering::Relaxed);
    println!("engine=openraft-disk");
    println!("wal_dir={}", wal_dir.display());
    println!("wal_fsync={:?}", args.wal_fsync);
    println!("core_count={}", args.core_count);
    println!("raft_group_count={}", args.raft_group_count);
    println!("stream_count={}", args.stream_count);
    println!("producer_count={}", args.producer_count);
    println!("payload_bytes={}", args.payload_bytes);
    println!("duration_secs={elapsed:.3}");
    println!("counted_appends={counted_appends}");
    println!("metrics_accepted_appends={}", snapshot.accepted_appends);
    println!(
        "appends_per_sec={:.2}",
        snapshot.accepted_appends as f64 / elapsed
    );
    println!("routed_requests={}", snapshot.routed_requests);
    println!(
        "routed_requests_per_sec={:.2}",
        snapshot.routed_requests as f64 / elapsed
    );
    println!("active_cores={active_cores}");
    println!("active_groups={active_groups}");
    println!("mailbox_full_events={}", snapshot.mailbox_full_events);
    println!("group_mailbox_depth={}", snapshot.group_mailbox_depth);
    println!(
        "group_mailbox_max_depth={}",
        snapshot.group_mailbox_max_depth
    );
    println!("raft_apply_ns={}", snapshot.raft_apply_ns);
    println!("per_core_appends={:?}", snapshot.per_core_appends);
    println!(
        "per_core_routed_requests={:?}",
        snapshot.per_core_routed_requests
    );
    println!("mailbox_depths={:?}", runtime.mailbox_snapshot().depths);
    runtime.shutdown_group_engines().await?;
    wal.shutdown().await?;
    if remove_wal_dir {
        std::fs::remove_dir_all(&wal_dir)?;
    }
    Ok(())
}

#[cfg(not(madsim))]
async fn create_streams(
    runtime: &ShardRuntime,
    streams: &[BucketStreamId],
    setup_concurrency: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let setup_concurrency = setup_concurrency.max(1);
    for batch in streams.chunks(setup_concurrency) {
        let mut tasks = JoinSet::new();
        for stream in batch.iter().cloned() {
            let runtime = runtime.clone();
            tasks.spawn(async move {
                runtime
                    .create_stream(CreateStreamRequest::new(stream, DEFAULT_CONTENT_TYPE))
                    .await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
#[cfg(not(madsim))]
struct Args {
    core_count: usize,
    raft_group_count: usize,
    stream_count: usize,
    producer_count: usize,
    setup_concurrency: usize,
    mailbox_capacity: usize,
    payload_bytes: usize,
    duration: Duration,
    wal_dir: Option<PathBuf>,
    wal_fsync: WalFsync,
}

#[cfg(not(madsim))]
impl Args {
    fn parse() -> Result<Self, String> {
        let core_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        let mut args = Self {
            core_count,
            raft_group_count: core_count.saturating_mul(16).max(1),
            stream_count: 4096,
            producer_count: core_count.saturating_mul(64).max(1),
            setup_concurrency: 1024,
            mailbox_capacity: 1024,
            payload_bytes: 100,
            duration: Duration::from_secs(10),
            wal_dir: None,
            wal_fsync: WalFsync::Never,
        };

        let mut raw_args = std::env::args().skip(1);
        while let Some(arg) = raw_args.next() {
            match arg.as_str() {
                "--core-count" => {
                    args.core_count = parse_next(&mut raw_args, "--core-count")?;
                }
                "--raft-group-count" => {
                    args.raft_group_count = parse_next(&mut raw_args, "--raft-group-count")?;
                }
                "--stream-count" => {
                    args.stream_count = parse_next(&mut raw_args, "--stream-count")?;
                }
                "--producer-count" => {
                    args.producer_count = parse_next(&mut raw_args, "--producer-count")?;
                }
                "--setup-concurrency" => {
                    args.setup_concurrency = parse_next(&mut raw_args, "--setup-concurrency")?;
                }
                "--mailbox-capacity" => {
                    args.mailbox_capacity = parse_next(&mut raw_args, "--mailbox-capacity")?;
                }
                "--payload-bytes" => {
                    args.payload_bytes = parse_next(&mut raw_args, "--payload-bytes")?;
                }
                "--duration-secs" => {
                    let seconds = parse_next::<f64>(&mut raw_args, "--duration-secs")?;
                    args.duration = Duration::from_secs_f64(seconds);
                }
                "--wal-dir" => {
                    args.wal_dir = Some(parse_next(&mut raw_args, "--wal-dir")?);
                }
                "--wal-fsync" => {
                    args.wal_fsync =
                        match parse_next::<String>(&mut raw_args, "--wal-fsync")?.as_str() {
                            "always" => WalFsync::Always,
                            "never" => WalFsync::Never,
                            other => {
                                return Err(format!(
                                    "invalid --wal-fsync '{other}': expected always or never"
                                ));
                            }
                        };
                }
                "--help" | "-h" => return Err(help()),
                other => return Err(format!("unknown argument '{other}'\n\n{}", help())),
            }
        }

        if args.core_count == 0 {
            return Err("--core-count must be greater than zero".to_owned());
        }
        if args.raft_group_count == 0 {
            return Err("--raft-group-count must be greater than zero".to_owned());
        }
        if args.stream_count == 0 {
            return Err("--stream-count must be greater than zero".to_owned());
        }
        if args.producer_count == 0 {
            return Err("--producer-count must be greater than zero".to_owned());
        }
        if args.payload_bytes == 0 {
            return Err("--payload-bytes must be greater than zero".to_owned());
        }
        if args.duration.is_zero() {
            return Err("--duration-secs must be greater than zero".to_owned());
        }

        Ok(args)
    }
}

#[cfg(not(madsim))]
fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    let raw = args
        .next()
        .ok_or_else(|| format!("{name} requires a value"))?;
    raw.parse()
        .map_err(|err| format!("invalid {name} '{raw}': {err}"))
}

#[cfg(not(madsim))]
fn help() -> String {
    "usage: ursula-raft-runtime-stress [--core-count N] [--raft-group-count N] [--stream-count N] [--producer-count N] [--setup-concurrency N] [--mailbox-capacity N] [--payload-bytes N] [--duration-secs N] [--wal-dir PATH] [--wal-fsync always|never]".to_owned()
}
