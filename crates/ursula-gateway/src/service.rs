use std::net::SocketAddr;
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
use crate::usage::JsonlUsageSink;
use crate::usage::UsageCollector;

const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 3600;

pub async fn run(args: GatewayArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut init_options = ursula_observability::InitOptions::new("ursula-gateway");
    init_options.default_directives = "ursula_gateway=info";
    let _observability = ursula_observability::init(init_options);

    let installed_access_control = access_control(&args)?;
    let config = GatewayConfig {
        listen: args.listen,
        upstreams: args.upstream,
        response_header_timeout: Duration::from_secs(args.response_header_timeout),
        connect_timeout: Duration::from_secs(args.connect_timeout),
        max_request_body_bytes: args.max_request_body_bytes,
        raft_group_count: args.raft_group_count.map(NonZeroUsize::get),
        cors_allowed_origins: args.cors_allowed_origin.clone(),
    };

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

    let gateway = Arc::new(gateway);

    // Keep Axum's State extractor in the binary so the library handler stays
    // easy to call directly from tests.
    let app = Router::new()
        .fallback(any(
            |State(gateway): State<Arc<Gateway>>, req: Request<Body>| async move {
                gateway.handle(req).await
            },
        ))
        .with_state(gateway);

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

    tracing::info!(policy = %policy_path.display(), "gateway access control enabled");
    Ok(Some(AccessControl::new(
        Arc::new(resolver),
        Arc::new(authorizer),
    )))
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

    /// Maximum request body bytes buffered for leader-redirect replay.
    #[arg(long, default_value_t = DEFAULT_MAX_REQUEST_BODY_BYTES)]
    max_request_body_bytes: usize,

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
    use std::time::Duration;

    use axum::Router;
    use tokio::sync::oneshot;
    use ursula_observability::serve::serve_until_shutdown;

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
