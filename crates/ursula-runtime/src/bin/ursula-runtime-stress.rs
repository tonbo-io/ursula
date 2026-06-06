// The stress binary exercises the production ThreadPerCore runtime; that
// variant is cfg(not(madsim))-only by design (DoD #1). Under cfg(madsim) the
// bin is a no-op so workspace builds stay green.

#[cfg(madsim)]
fn main() {}

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
use ursula_runtime::AppendBatchRequest;
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
    let runtime = ShardRuntime::spawn(config)?;

    let streams = (0..args.stream_count)
        .map(|index| BucketStreamId::new("stress", format!("stream-{index}")))
        .collect::<Vec<_>>();
    create_streams(&runtime, &streams, args.setup_concurrency).await?;

    let total_appends = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + args.duration;
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for producer_index in 0..args.producer_count {
        let runtime = runtime.clone();
        let streams = streams.clone();
        let total_appends = total_appends.clone();
        let args = args.clone();
        tasks.spawn(async move {
            let payload = vec![0; args.payload_bytes];
            let mut stream_index = producer_index % streams.len();
            while Instant::now() < deadline {
                let stream = streams[stream_index].clone();
                stream_index += args.producer_count;
                if stream_index >= streams.len() {
                    stream_index %= streams.len();
                }

                let accepted = match args.mode {
                    StressMode::Append => {
                        let mut request = AppendRequest::from_bytes(stream, payload.clone());
                        request.content_type = DEFAULT_CONTENT_TYPE.to_owned();
                        runtime.append(request).await.map(|_| 1)
                    }
                    StressMode::Batch => {
                        let mut request =
                            AppendBatchRequest::new(stream, vec![payload.clone(); args.batch_size]);
                        request.content_type = DEFAULT_CONTENT_TYPE.to_owned();
                        runtime.append_batch(request).await.map(|response| {
                            response.items.iter().filter(|item| item.is_ok()).count()
                        })
                    }
                }?;
                total_appends.fetch_add(
                    u64::try_from(accepted).expect("accepted count fits u64"),
                    Ordering::Relaxed,
                );
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
    println!("mode={}", args.mode.as_str());
    println!("core_count={}", args.core_count);
    println!("raft_group_count={}", args.raft_group_count);
    println!("stream_count={}", args.stream_count);
    println!("producer_count={}", args.producer_count);
    println!("batch_size={}", args.batch_size);
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
    println!("per_core_appends={:?}", snapshot.per_core_appends);
    println!(
        "per_core_routed_requests={:?}",
        snapshot.per_core_routed_requests
    );
    println!("mailbox_depths={:?}", runtime.mailbox_snapshot().depths);
    Ok(())
}

#[cfg(not(madsim))]
async fn create_streams(
    runtime: &ShardRuntime,
    streams: &[BucketStreamId],
    setup_concurrency: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let setup_concurrency = setup_concurrency.max(1);
    let mut next_stream = 0usize;
    while next_stream < streams.len() {
        let mut tasks = JoinSet::new();
        for stream in streams
            .iter()
            .skip(next_stream)
            .take(setup_concurrency)
            .cloned()
        {
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
        next_stream += setup_concurrency;
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
    batch_size: usize,
    payload_bytes: usize,
    duration: Duration,
    mode: StressMode,
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
            batch_size: 16,
            payload_bytes: 128,
            duration: Duration::from_secs(10),
            mode: StressMode::Batch,
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
                "--batch-size" => {
                    args.batch_size = parse_next(&mut raw_args, "--batch-size")?;
                }
                "--payload-bytes" => {
                    args.payload_bytes = parse_next(&mut raw_args, "--payload-bytes")?;
                }
                "--duration-secs" => {
                    let seconds = parse_next::<f64>(&mut raw_args, "--duration-secs")?;
                    args.duration = Duration::from_secs_f64(seconds);
                }
                "--mode" => {
                    args.mode = parse_next::<StressMode>(&mut raw_args, "--mode")?;
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
        if args.batch_size == 0 {
            return Err("--batch-size must be greater than zero".to_owned());
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(not(madsim))]
enum StressMode {
    Append,
    Batch,
}

#[cfg(not(madsim))]
impl StressMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Batch => "batch",
        }
    }
}

#[cfg(not(madsim))]
impl std::str::FromStr for StressMode {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "append" => Ok(Self::Append),
            "batch" => Ok(Self::Batch),
            _ => Err("expected append or batch".to_owned()),
        }
    }
}

#[cfg(not(madsim))]
fn help() -> String {
    "usage: ursula-runtime-stress [--mode append|batch] [--core-count N] [--raft-group-count N] [--stream-count N] [--producer-count N] [--setup-concurrency N] [--mailbox-capacity N] [--batch-size N] [--payload-bytes N] [--duration-secs N]".to_owned()
}
