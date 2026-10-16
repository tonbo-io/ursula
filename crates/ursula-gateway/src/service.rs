use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::routing::any;
use clap::Args;
use ursula_observability::serve::serve_until_shutdown;
use ursula_observability::serve::shutdown_signal;

use crate::DEFAULT_MAX_REQUEST_BODY_BYTES;
use crate::Gateway;
use crate::GatewayConfig;
use crate::admission::StaticQuotaProvider;
use crate::auth::AccessControl;
use crate::auth::jwt::JwtPrincipalResolver;
use crate::auth::jwt::JwtValidationConfig;
use crate::auth::policy::StaticPolicyAuthorizer;
use crate::uploads::DEFAULT_BUDGET_WAIT;
use crate::uploads::DEFAULT_MIN_BYTES_PER_SEC;
use crate::uploads::DEFAULT_STALL_TIMEOUT;
use crate::uploads::UploadLimits;
use crate::uploads::UploadLimitsError;
use crate::uploads::UploadLimitsSpec;
use crate::usage::JsonlUsageSink;
use crate::usage::UsageCollector;

const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 3600;
/// The SQLite extension's timeout for a commit's request.
const SQLITE_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(args: GatewayArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut init_options = ursula_observability::InitOptions::new("ursula-gateway");
    init_options.default_directives = "ursula_gateway=info";
    let _observability = ursula_observability::init(init_options);

    let installed_access_control = access_control(&args)?;
    let config = gateway_config(&args)?;
    warn_on_budget_over_memory(&config.uploads);
    if config.uploads.stall_timeout() <= SQLITE_COMMIT_TIMEOUT {
        tracing::warn!(
            stall_timeout_secs = config.uploads.stall_timeout().as_secs(),
            "--body-stall-timeout is not above the SQLite extension's 10 s commit timeout: the \
             gateway may cut a commit its client still waits for, which the client sees as an \
             unknown outcome"
        );
    }

    let mut gateway = match installed_access_control {
        Some(access_control) => Gateway::with_access_control(config.clone(), access_control),
        None => Gateway::new(config.clone()),
    };

    if let Some(policy_path) = &args.quota_policy {
        let provider = StaticQuotaProvider::from_file(policy_path)?;
        tracing::info!(policy = %policy_path.display(), "gateway quotas enabled");
        gateway = gateway.with_quota_provider(Arc::new(provider));
    }

    let mut usage_shutdown = None;
    let mut usage_exporter = None;
    if let Some(usage_path) = &args.usage_log {
        let sink = Arc::new(JsonlUsageSink::create(usage_path)?);
        let collector = UsageCollector::new();
        gateway = gateway.with_usage_collector(Arc::clone(&collector));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        usage_exporter = Some(tokio::spawn(crate::usage::run_exporter(
            collector,
            sink,
            Duration::from_secs(args.usage_flush_secs),
            shutdown_rx,
        )));
        usage_shutdown = Some(shutdown_tx);
        tracing::info!(log = %usage_path.display(), "gateway usage accounting enabled");
    }

    let app = router(Arc::new(gateway));

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(
        listen = %config.listen,
        upstreams = ?config.upstreams,
        "Ursula gateway starting"
    );

    serve_until_shutdown(
        listener,
        app,
        shutdown_signal(),
        Some(Duration::from_secs(args.graceful_shutdown_timeout)),
    )
    .await?;

    // Stop the exporter after the server drains so the final usage window is
    // flushed rather than stranded.
    if let Some(shutdown) = usage_shutdown {
        drop(shutdown);
    }
    if let Some(exporter) = usage_exporter {
        let _joined = exporter.await;
    }

    Ok(())
}

/// The gateway's HTTP service: every request goes to [`Gateway::handle`].
/// Axum's State extractor stays here, out of the library handler, so tests
/// can call the handler directly.
pub(crate) fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .fallback(any(
            |State(gateway): State<Arc<Gateway>>, req: Request<Body>| async move {
                gateway.handle(req).await
            },
        ))
        .with_state(gateway)
}

/// Builds the opt-in access-control hooks from CLI arguments. All-absent means
/// the gateway keeps its original trusted pass-through behavior.
fn access_control(args: &GatewayArgs) -> Result<Option<AccessControl>, Box<dyn std::error::Error>> {
    let Some(issuer) = args.auth_issuer.clone() else {
        return Ok(None);
    };
    // clap's `requires` links make these present whenever the issuer is set.
    let audience = args
        .auth_audience
        .clone()
        .ok_or("--auth-audience is required with --auth-issuer")?;
    let policy_path = args
        .auth_policy
        .as_ref()
        .ok_or("--auth-policy is required with --auth-issuer")?;

    let mut jwt_config = JwtValidationConfig::new(issuer, audience);
    jwt_config.jwks_url = args.auth_jwks_url.clone();
    let resolver = JwtPrincipalResolver::new(jwt_config)?;
    let authorizer = StaticPolicyAuthorizer::from_file(policy_path)?;

    tracing::info!(
        policy = %policy_path.display(),
        public_metrics = args.auth_public_metrics,
        "gateway access control enabled"
    );
    Ok(Some(
        AccessControl::new(Arc::new(resolver), Arc::new(authorizer))
            .with_public_metrics(args.auth_public_metrics),
    ))
}

/// The body budget: `--body-budget-bytes` when set, which the limits then
/// check against `--max-request-body-bytes`. Unset, the default budget, grown
/// to hold a body at the limit beside the headroom, so that raising the limit
/// alone never stops a gateway from starting.
fn body_budget_bytes(args: &GatewayArgs) -> u64 {
    if let Some(budget) = args.body_budget_bytes {
        return budget;
    }
    let default = UploadLimitsSpec::default().budget_bytes;
    let needed = UploadLimitsSpec {
        max_request_body_bytes: args.max_request_body_bytes,
        small_body_headroom_bytes: args.small_body_headroom_bytes,
        ..UploadLimitsSpec::default()
    }
    .smallest_budget_bytes();
    if needed > default {
        tracing::warn!(
            budget_bytes = needed,
            "--body-budget-bytes is unset; the body budget grows to hold a body at \
             --max-request-body-bytes beside --small-body-headroom-bytes, and the gateway needs \
             about twice the budget in memory. No budget below that can be set: lower \
             --max-request-body-bytes or give the gateway more memory"
        );
    }
    default.max(needed)
}

/// The cgroup v2 and v1 hierarchies, where `/proc/self/cgroup` paths start.
const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";
const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";

/// The files that may state a memory limit of the process, from the
/// contents of `/proc/self/cgroup`: those of its cgroup and of each of its
/// ancestors, in v2 (`0::<path>`) and in the v1 memory controller
/// (`<id>:memory:<path>`), the roots included. Inside a container without a
/// cgroup namespace, the process's path does not exist under the container's
/// mount, whose root then states the container's limit.
fn cgroup_memory_limit_files(proc_self_cgroup: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for line in proc_self_cgroup.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(_id), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let (root, file) = if controllers.is_empty() {
            (CGROUP_V2_ROOT, "memory.max")
        } else if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            (CGROUP_V1_MEMORY_ROOT, "memory.limit_in_bytes")
        } else {
            continue;
        };
        let mut dir = Some(std::path::Path::new(path));
        while let Some(at) = dir {
            let relative = at.strip_prefix("/").unwrap_or(at);
            files.push(std::path::Path::new(root).join(relative).join(file));
            dir = at.parent();
        }
    }
    if files.is_empty() {
        files.push(std::path::Path::new(CGROUP_V2_ROOT).join("memory.max"));
        files.push(std::path::Path::new(CGROUP_V1_MEMORY_ROOT).join("memory.limit_in_bytes"));
    }
    files
}

/// A cgroup memory limit file's contents as bytes: `None` for `max` (v2) or
/// an unset v1 limit, which the kernel reports as a value near `i64::MAX`.
fn parse_cgroup_memory_limit(contents: &str) -> Option<u64> {
    let limit = contents.trim().parse::<u64>().ok()?;
    (limit < 1 << 60).then_some(limit)
}

/// The process's cgroup memory limit: the smallest that its cgroup or an
/// ancestor states ([`cgroup_memory_limit_files`]), or `None` where none
/// does or the files cannot be read.
fn cgroup_memory_limit() -> Option<u64> {
    let proc_self_cgroup = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    cgroup_memory_limit_files(&proc_self_cgroup)
        .iter()
        .filter_map(|file| std::fs::read_to_string(file).ok())
        .filter_map(|contents| parse_cgroup_memory_limit(&contents))
        .min()
}

/// Warns when the body budget exceeds half of the process's cgroup memory
/// limit. A budget that size leaves too little for everything else the
/// gateway holds.
fn warn_on_budget_over_memory(limits: &UploadLimits) {
    let Some(limit) = cgroup_memory_limit() else {
        return;
    };
    let budget_bytes = limits.budget_bytes();
    if budget_bytes > limit / 2 {
        tracing::warn!(
            budget_bytes,
            memory_limit_bytes = limit,
            max_request_body_bytes = limits.max_request_body_bytes(),
            "the request body budget exceeds half of the cgroup memory limit. Give the gateway \
             about twice the budget in memory, or lower the budget, and with it \
             --max-request-body-bytes: the budget cannot go below the limit plus \
             --small-body-headroom-bytes"
        );
    }
}

fn gateway_config(args: &GatewayArgs) -> Result<GatewayConfig, UploadLimitsError> {
    Ok(GatewayConfig {
        listen: args.listen,
        upstreams: args.upstream.clone(),
        response_header_timeout: Duration::from_secs(args.response_header_timeout),
        connect_timeout: Duration::from_secs(args.connect_timeout),
        upstream_tcp_user_timeout_secs: args.upstream_tcp_user_timeout,
        uploads: UploadLimits::try_from(UploadLimitsSpec {
            max_request_body_bytes: args.max_request_body_bytes,
            budget_bytes: body_budget_bytes(args),
            small_body_headroom_bytes: args.small_body_headroom_bytes,
            small_body_max_bytes: args.small_body_max_bytes,
            budget_wait: Duration::from_millis(args.body_budget_wait_ms),
            stall_timeout: Duration::from_secs(args.body_stall_timeout),
            min_bytes_per_sec: args.body_min_bytes_per_sec,
        })?,
        raft_group_count: args.raft_group_count.map(NonZeroUsize::get),
        cors_allowed_origins: args.cors_allowed_origin.clone(),
        usage_chunk_bytes: args.usage_chunk_bytes.map(NonZeroU64::get),
    })
}

#[derive(Args, Debug)]
pub struct GatewayArgs {
    /// Address to bind the gateway server.
    #[arg(long, default_value = "0.0.0.0:4437")]
    listen: SocketAddr,

    /// Upstream Ursula node URL. Repeat for each node.
    #[arg(long, required = true)]
    upstream: Vec<String>,

    /// Timeout for sending the upstream request and receiving response headers, in seconds.
    /// Streamed response bodies such as SSE live reads are not covered by this timeout.
    #[arg(long, default_value_t = 30)]
    response_header_timeout: u64,

    /// TCP connect timeout per upstream attempt in seconds.
    #[arg(long, default_value_t = 5)]
    connect_timeout: u64,

    /// Seconds data sent to an upstream may stay unacknowledged before the
    /// connection is closed and its request fails with 502 (Linux
    /// `TCP_USER_TIMEOUT`). Bounds how long a request waits on a connection
    /// to a node that vanished without closing it, such as a force-deleted
    /// pod or a powered-off host. Idle connections are probed three times
    /// within it. At least 1. 0.7.0 effectively used reqwest's 30 s default.
    #[arg(long, default_value = "5")]
    upstream_tcp_user_timeout: NonZeroU64,

    /// Maximum request body bytes buffered for leader-redirect replay. Also
    /// the most the gateway reads of a body before it sends a refusal. At
    /// most the protocol's request limit of 32 MiB, which every node
    /// applies: a larger value refuses to start. Must fit the part of
    /// `--body-budget-bytes` large bodies may take, the budget less
    /// `--small-body-headroom-bytes`.
    #[arg(long, default_value_t = DEFAULT_MAX_REQUEST_BODY_BYTES)]
    max_request_body_bytes: usize,

    /// Bytes of request bodies the gateway buffers at once, across requests,
    /// the small-body headroom included. Each body reserves its declared
    /// length before the gateway reads it, and holds it until it was
    /// forwarded. A body without a declared length reserves
    /// `--small-body-max-bytes` at first and more as it arrives. The default,
    /// 256 MiB, is half of a 512 MiB pod: seven bodies at the default
    /// per-request limit beside the headroom. Keep it well below the memory
    /// limit. Unset, it grows to hold a body at `--max-request-body-bytes`
    /// beside the headroom when that limit is raised past the default budget.
    #[arg(long, value_name = "BYTES")]
    body_budget_bytes: Option<u64>,

    /// Bytes of the body budget that only bodies of at most
    /// `--small-body-max-bytes` may take, so that small writes never queue
    /// behind large uploads. Must hold at least one small body. The default,
    /// 32 MiB, holds at least 32 small bodies.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimitsSpec::default().small_body_headroom_bytes)]
    small_body_headroom_bytes: u64,

    /// The largest declared body length that counts as small: it may take
    /// the small-body headroom, and it never counts toward a bucket's upload
    /// limits. A body without a declared length counts as small until it
    /// grows past it.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimitsSpec::default().small_body_max_bytes)]
    small_body_max_bytes: u64,

    /// Milliseconds a request waits for the body budget before the gateway
    /// answers 503 with Retry-After. 0 refuses at once.
    #[arg(
        long,
        value_name = "MILLISECONDS",
        default_value_t = u64::try_from(DEFAULT_BUDGET_WAIT.as_millis()).unwrap_or(u64::MAX)
    )]
    body_budget_wait_ms: u64,

    /// Seconds a request body may go without a byte arriving before the
    /// gateway cuts it and answers 408, with nothing of it forwarded, and
    /// the length of the windows `--body-min-bytes-per-sec` is measured
    /// over. At least 1. The default, 30, is past the SQLite extension's
    /// 10 s timeout for a commit: only a body whose client went away without
    /// closing the connection stalls this long. Keep it above 10 s, or the
    /// gateway may cut a commit its client still waits for.
    #[arg(long, value_name = "SECONDS", default_value_t = DEFAULT_STALL_TIMEOUT.as_secs())]
    body_stall_timeout: u64,

    /// The least rate at which a request body must arrive, measured over
    /// each window of `--body-stall-timeout` from the start of its reading,
    /// not over the whole body: a body that delivers fewer bytes in one
    /// window is cut and answered 408, with nothing of it forwarded. Nothing
    /// bounds a body's reading in all, so a slow upload that keeps arriving
    /// is never cut. 0 disables the floor. The default, 1024, cuts only
    /// connections that trickle a byte now and then.
    #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MIN_BYTES_PER_SEC)]
    body_min_bytes_per_sec: u64,

    /// Number of Raft groups in the upstream Ursula cluster. When set, leader
    /// affinity is shared by every stream that hashes to the same group.
    #[arg(long)]
    raft_group_count: Option<NonZeroUsize>,

    /// Maximum graceful shutdown drain time after SIGTERM/CTRL-C, in seconds.
    #[arg(long, default_value_t = DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_SECS)]
    graceful_shutdown_timeout: u64,

    /// OAuth issuer URL expected in access-token `iss` claims. Setting this
    /// enables gateway access control; leaving it unset keeps the original
    /// trusted pass-through behavior.
    #[arg(long, requires = "auth_audience", requires = "auth_policy")]
    auth_issuer: Option<String>,

    /// OAuth audience expected in access-token `aud` claims.
    #[arg(long, requires = "auth_issuer")]
    auth_audience: Option<String>,

    /// Explicit JWKS document URL. Defaults to RFC 8414 metadata discovery
    /// under the issuer.
    #[arg(long, requires = "auth_issuer")]
    auth_jwks_url: Option<String>,

    /// TOML bucket policy file declaring owners and public-read visibility.
    #[arg(long, requires = "auth_issuer")]
    auth_policy: Option<PathBuf>,

    /// Answer `GET /__ursula/gateway/metrics` with the gateway's own counters
    /// for every caller, with or without a token, as a gateway without access
    /// control does. The gateway does not check a presented token there, so
    /// an invalid or expired one gets the counters too. Requires access
    /// control (`--auth-issuer`). Without this flag an access-controlled
    /// gateway answers that path 404, like every other `/__ursula/*` path.
    /// The counters hold no tenant data, but they show traffic volume: set
    /// the flag only where untrusted clients cannot reach the listener, or
    /// where a proxy in front of the gateway denies `/__ursula/*`.
    #[arg(long, requires = "auth_issuer")]
    auth_public_metrics: bool,

    /// TOML per-tenant quota policy. Requires access control: limits are
    /// keyed by the classified bucket resource.
    #[arg(long, requires = "auth_issuer")]
    quota_policy: Option<PathBuf>,

    /// Append per-tenant usage batches to this JSONL file. Requires access
    /// control for the same reason as quotas.
    #[arg(long, requires = "auth_issuer")]
    usage_log: Option<PathBuf>,

    /// Interval between usage exports, in seconds.
    #[arg(long, default_value_t = 30, requires = "usage_log")]
    usage_flush_secs: u64,

    /// Count append bytes in units of this size, as `ceil(bytes / unit)` with
    /// a minimum of one, and report the sum as `chunks`. Unset leaves that
    /// counter at zero.
    ///
    /// For deployments that charge per write unit rather than per byte. The
    /// size is a pricing choice — 10 KB and 25 KB are both in use — so this
    /// crate does not pick one. A batched append costs proportionally less
    /// than the same records sent one at a time, which is the point: batching
    /// is cheaper to serve.
    #[arg(long, value_name = "BYTES", requires = "usage_log")]
    usage_chunk_bytes: Option<NonZeroU64>,

    /// Origin allowed to read across origins. Repeat per origin, or pass `*`.
    /// Unset disables CORS, which leaves anonymous `public_read` buckets
    /// unreachable from browser JavaScript. Credentials are never allowed, so
    /// `*` grants no ambient access: a cross-origin caller must still present
    /// its own bearer token.
    #[arg(long, value_name = "ORIGIN")]
    cors_allowed_origin: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use clap::Parser;
    use tokio::sync::oneshot;
    use ursula_observability::serve::serve_until_shutdown;

    use super::GatewayArgs;
    use super::access_control;
    use super::cgroup_memory_limit_files;
    use super::gateway_config;
    use super::parse_cgroup_memory_limit;
    use crate::Gateway;
    use crate::uploads::UploadLimits;
    use crate::uploads::UploadLimitsError;
    use crate::uploads::UploadLimitsSpec;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: GatewayArgs,
    }

    fn config_from(extra: &[&str]) -> crate::GatewayConfig {
        let argv = ["ursulagw", "--upstream", "http://node:4437"]
            .iter()
            .chain(extra);
        gateway_config(&Cli::try_parse_from(argv).unwrap().args).unwrap()
    }

    // A request on a connection to a vanished upstream fails after 5 s unless
    // configured otherwise (#495). There is no "off": 0 is refused.
    #[test]
    fn upstream_tcp_user_timeout_defaults_to_five_seconds_and_refuses_zero() {
        assert_eq!(
            config_from(&[]).upstream_tcp_user_timeout_secs,
            NonZeroU64::new(5).unwrap()
        );
        assert_eq!(
            config_from(&["--upstream-tcp-user-timeout", "30"]).upstream_tcp_user_timeout_secs,
            NonZeroU64::new(30).unwrap()
        );
        let refused = Cli::try_parse_from([
            "ursulagw",
            "--upstream",
            "http://node:4437",
            "--upstream-tcp-user-timeout",
            "0",
        ]);
        assert!(matches!(
            refused.map(|_| ()).unwrap_err().kind(),
            clap::error::ErrorKind::ValueValidation
        ));
    }

    // The memory limit is looked up along the process's own cgroup path and
    // its ancestors, v2 and the v1 memory controller, as a systemd unit's
    // `MemoryMax=` sets it below the root.
    #[test]
    fn cgroup_memory_limit_files_follow_the_process_cgroup_path() {
        let files = |contents: &str| -> Vec<String> {
            cgroup_memory_limit_files(contents)
                .iter()
                .map(|file| file.display().to_string())
                .collect()
        };
        assert_eq!(files("0::/system.slice/ursulagw.service\n"), [
            "/sys/fs/cgroup/system.slice/ursulagw.service/memory.max",
            "/sys/fs/cgroup/system.slice/memory.max",
            "/sys/fs/cgroup/memory.max",
        ]);
        assert_eq!(files("12:cpu,cpuacct:/pod\n4:memory:/pod/gw\n"), [
            "/sys/fs/cgroup/memory/pod/gw/memory.limit_in_bytes",
            "/sys/fs/cgroup/memory/pod/memory.limit_in_bytes",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]);
        assert_eq!(files(""), [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]);
    }

    #[test]
    fn a_cgroup_memory_limit_parses_and_an_unset_one_is_none() {
        assert_eq!(parse_cgroup_memory_limit("536870912\n"), Some(512 << 20));
        assert_eq!(parse_cgroup_memory_limit("max\n"), None);
        assert_eq!(parse_cgroup_memory_limit("9223372036854771712\n"), None);
        assert_eq!(parse_cgroup_memory_limit(""), None);
    }

    // The body budget's flags default to the library's limits, and limits
    // that leave a body at the limit no room are refused at startup.
    #[test]
    fn upload_flags_default_to_the_library_limits_and_refuse_a_limit_above_the_protocols() {
        assert_eq!(config_from(&[]).uploads, UploadLimits::default());
        let parsed = |extra: &[&str]| {
            let argv = ["ursulagw", "--upstream", "http://node:4437"]
                .iter()
                .chain(extra);
            gateway_config(&Cli::try_parse_from(argv).unwrap().args).map(|config| config.uploads)
        };
        // A limit above the protocol's is refused at startup: the node
        // refuses such a body before reading it, and the gateway would
        // answer `502` instead of a typed `413`.
        assert_eq!(
            parsed(&["--max-request-body-bytes", "33554433"]),
            Err(UploadLimitsError::LimitAboveProtocol {
                max_request_body_bytes: (32 << 20) + 1,
                protocol_limit: 32 << 20,
            })
        );
        // Raising the headroom alone grows the budget to hold the limit
        // beside it, in whole KiB, so a headroom that is not whole KiB
        // still starts.
        assert_eq!(
            parsed(&["--small-body-headroom-bytes", "268435457"])
                .unwrap()
                .spec()
                .budget_bytes,
            (32_768 + 262_145) * 1024
        );
        // A budget set too small for the limit is refused.
        assert_eq!(
            parsed(&["--body-budget-bytes", "50331648"]),
            Err(UploadLimitsError::LimitAboveSharedBudget {
                max_request_body_bytes: 32 << 20,
                shared_bytes: 16 << 20,
            })
        );
        let raised = parsed(&[
            "--body-budget-bytes",
            "1207959552",
            "--body-stall-timeout",
            "10",
            "--body-min-bytes-per-sec",
            "0",
        ])
        .unwrap();
        assert_eq!(raised.spec(), &UploadLimitsSpec {
            budget_bytes: 1_207_959_552,
            stall_timeout: Duration::from_secs(10),
            min_bytes_per_sec: 0,
            ..UploadLimitsSpec::default()
        });
        assert_eq!(
            parsed(&["--body-stall-timeout", "0"]),
            Err(UploadLimitsError::ZeroStallTimeout)
        );
    }

    // An access-controlled gateway answers its metrics path 404 unless
    // `--auth-public-metrics` makes the metrics public (#548). The flag
    // requires `--auth-issuer`: a gateway without access control serves the
    // metrics anyway.
    #[tokio::test]
    async fn auth_public_metrics_serves_the_metrics_of_an_access_controlled_gateway() {
        let policy_file = tempfile::NamedTempFile::new().expect("an empty policy file");
        let policy = policy_file.path().to_str().expect("a UTF-8 path");
        let auth = [
            "--auth-issuer",
            "https://issuer.example",
            "--auth-audience",
            "https://streams.example",
            "--auth-policy",
            policy,
        ];
        for (flag, status) in [
            (None, StatusCode::NOT_FOUND),
            (Some("--auth-public-metrics"), StatusCode::OK),
        ] {
            let argv = ["ursulagw", "--upstream", "http://node:4437"]
                .into_iter()
                .chain(auth)
                .chain(flag);
            let args = Cli::try_parse_from(argv).unwrap().args;
            let gateway = Gateway::with_access_control(
                gateway_config(&args).unwrap(),
                access_control(&args).unwrap().expect("access control"),
            );
            let request = Request::get("/__ursula/gateway/metrics")
                .body(Body::empty())
                .unwrap();
            assert_eq!(gateway.handle(request).await.status(), status, "{flag:?}");
        }

        let refused = Cli::try_parse_from([
            "ursulagw",
            "--upstream",
            "http://node:4437",
            "--auth-public-metrics",
        ]);
        assert!(matches!(
            refused.map(|_| ()).unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        ));
    }

    #[tokio::test]
    async fn serve_with_shutdown_does_not_return_before_shutdown_signal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let app = Router::new();
        let (_tx, rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            async move {
                drop(rx.await);
            },
            Some(Duration::from_secs(60)),
        ));

        // Give the server a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The task should still be running since no shutdown signal was sent.
        assert!(!handle.is_finished());

        // Clean up.
        handle.abort();
    }

    #[tokio::test]
    async fn serve_with_shutdown_returns_promptly_after_shutdown_signal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let app = Router::new();
        let (tx, rx) = oneshot::channel();

        let handle = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            async move {
                drop(rx.await);
            },
            Some(Duration::from_secs(60)),
        ));

        tx.send(()).expect("send shutdown signal");

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("server should return promptly after shutdown signal")
            .expect("server task should not panic")
            .expect("server should stop cleanly");
    }

    #[tokio::test]
    async fn serve_with_shutdown_returns_ok_when_drain_timeout_elapses() {
        use axum::routing::get;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener local addr");

        // Handler that keeps the connection occupied for longer than the drain timeout.
        let app = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                "done"
            }),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(serve_until_shutdown(
            listener,
            app,
            async move {
                drop(shutdown_rx.await);
            },
            Some(Duration::from_millis(100)), // short drain timeout
        ));

        // Start a slow request so the server has an active connection.
        let client = reqwest::Client::new();
        let client_handle = tokio::spawn(async move {
            drop(client.get(format!("http://{addr}/slow")).send().await);
        });

        // Wait for the request to reach the handler.
        tokio::time::sleep(Duration::from_millis(50)).await;

        shutdown_tx.send(()).expect("send shutdown signal");

        // Server should return after drain_timeout despite the slow request.
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("server should return within 2s")
            .expect("server task should not panic");

        assert!(
            result.is_ok(),
            "server should return Ok when drain timeout elapses"
        );

        client_handle.abort();
    }
}
