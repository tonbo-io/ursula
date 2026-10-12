use std::net::SocketAddr;
use std::num::NonZeroU32;
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
use crate::uploads::DEFAULT_READ_GRACE;
use crate::uploads::UploadLimits;
use crate::usage::JsonlUsageSink;
use crate::usage::UsageCollector;

const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 3600;

pub async fn run(args: GatewayArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut init_options = ursula_observability::InitOptions::new("ursula-gateway");
    init_options.default_directives = "ursula_gateway=info";
    let _observability = ursula_observability::init(init_options);

    let installed_access_control = access_control(&args)?;
    let config = gateway_config(&args);
    config.uploads.check()?;

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

fn gateway_config(args: &GatewayArgs) -> GatewayConfig {
    GatewayConfig {
        listen: args.listen,
        upstreams: args.upstream.clone(),
        response_header_timeout: Duration::from_secs(args.response_header_timeout),
        connect_timeout: Duration::from_secs(args.connect_timeout),
        upstream_tcp_user_timeout_secs: args.upstream_tcp_user_timeout,
        max_request_body_bytes: args.max_request_body_bytes,
        uploads: UploadLimits {
            budget_bytes: args.body_budget_bytes,
            small_body_headroom_bytes: args.small_body_headroom_bytes,
            small_body_max_bytes: args.small_body_max_bytes,
            tenant_bytes: args.tenant_upload_bytes,
            tenant_uploads: args.tenant_uploads,
            budget_wait: Duration::from_millis(args.body_budget_wait_ms),
            read_grace: Duration::from_secs(args.body_read_grace),
            min_bytes_per_sec: args.body_min_bytes_per_sec,
        },
        raft_group_count: args.raft_group_count.map(NonZeroUsize::get),
        cors_allowed_origins: args.cors_allowed_origin.clone(),
        usage_chunk_bytes: args.usage_chunk_bytes.map(NonZeroU64::get),
    }
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
    /// the most the gateway reads of a body before it sends a refusal it
    /// decided from the request's head alone.
    #[arg(long, default_value_t = DEFAULT_MAX_REQUEST_BODY_BYTES)]
    max_request_body_bytes: usize,

    /// Bytes of request bodies the gateway buffers at once, across requests,
    /// the small-body headroom included. Each body reserves its declared
    /// length, or `--max-request-body-bytes` when it has none, before the
    /// gateway reads it, and holds it until it was forwarded. The default,
    /// 256 MiB, is half of a 512 MiB pod: seven bodies at the default
    /// per-request limit beside the headroom. Raise it with
    /// `--max-request-body-bytes`, and keep it well below the memory limit.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimits::default().budget_bytes)]
    body_budget_bytes: NonZeroU64,

    /// Bytes of the body budget that only bodies of at most
    /// `--small-body-max-bytes` may take, so that small writes never queue
    /// behind large uploads. Must be below `--body-budget-bytes`. The
    /// default, 32 MiB, holds at least 32 small bodies.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimits::default().small_body_headroom_bytes)]
    small_body_headroom_bytes: u64,

    /// The largest declared body length that may take the small-body
    /// headroom. A body without a declared length counts as large.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimits::default().small_body_max_bytes)]
    small_body_max_bytes: u64,

    /// Bytes of the body budget one tenant, a bucket and the principal that
    /// authenticated the request, may hold at once. A tenant's first body
    /// always passes. Past it the gateway answers 429. The default, 64 MiB,
    /// is two bodies at the default per-request limit and a quarter of the
    /// default budget, so that no tenant takes the budget alone.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimits::default().tenant_bytes)]
    tenant_upload_bytes: NonZeroU64,

    /// Request bodies one tenant may have buffered or arriving at once. Past
    /// it the gateway answers 429.
    #[arg(long, value_name = "COUNT", default_value_t = UploadLimits::default().tenant_uploads)]
    tenant_uploads: NonZeroU32,

    /// Milliseconds a request waits for the body budget before the gateway
    /// answers 503 with Retry-After. 0 refuses at once.
    #[arg(
        long,
        value_name = "MILLISECONDS",
        default_value_t = u64::try_from(DEFAULT_BUDGET_WAIT.as_millis()).unwrap_or(u64::MAX)
    )]
    body_budget_wait_ms: u64,

    /// Seconds a request body may take to arrive whatever its pace. The
    /// default, 15, is past the SQLite extension's 10 s request timeout for
    /// a commit.
    #[arg(long, value_name = "SECONDS", default_value_t = DEFAULT_READ_GRACE.as_secs())]
    body_read_grace: u64,

    /// After `--body-read-grace`, the least average rate, since the reading
    /// started, at which a request body must arrive. A slower body is cut
    /// and answered 408 before anything of it is forwarded. The default,
    /// 256 KiB/s, lets a 32 MiB body take 128 s, past the SQLite extension's
    /// 120 s timeout for snapshot transfers.
    #[arg(long, value_name = "BYTES", default_value_t = UploadLimits::default().min_bytes_per_sec)]
    body_min_bytes_per_sec: NonZeroU64,

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
    use super::gateway_config;
    use crate::Gateway;
    use crate::uploads::UploadLimits;
    use crate::uploads::UploadLimitsError;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: GatewayArgs,
    }

    fn config_from(extra: &[&str]) -> crate::GatewayConfig {
        let argv = ["ursulagw", "--upstream", "http://node:4437"]
            .iter()
            .chain(extra);
        gateway_config(&Cli::try_parse_from(argv).unwrap().args)
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

    // The body budget's flags default to the library's limits, and a
    // headroom that leaves nothing of the budget for large bodies is refused
    // at startup.
    #[test]
    fn upload_flags_default_to_the_library_limits_and_refuse_a_full_headroom() {
        let defaults = config_from(&[]).uploads;
        assert_eq!(defaults, UploadLimits::default());
        defaults.check().unwrap();

        let full = config_from(&[
            "--body-budget-bytes",
            "1048576",
            "--small-body-headroom-bytes",
            "1048576",
        ])
        .uploads;
        assert!(matches!(
            full.check(),
            Err(UploadLimitsError::HeadroomNotBelowBudget {
                headroom_bytes: 1_048_576,
                ..
            })
        ));
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
                gateway_config(&args),
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
