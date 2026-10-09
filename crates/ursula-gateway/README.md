# Ursula Gateway

The `ursula gateway` role is a small HTTP/SSE gateway for Ursula clusters. It gives public
clients one stable HTTP endpoint while keeping internal Ursula node addresses
out of public responses.

The gateway is intentionally thin: it does not own stream routing state or
terminate SSE streams. It keeps two things in memory: the Raft leader it last
learned for each stream (or each Raft group, with `--raft-group-count`), and
which upstreams it is skipping after transport failures. It buffers request
bodies up to a configured limit only so it can replay requests when Ursula
identifies the Raft leader.

## Behavior

- Sends each incoming request to the cached leader of its stream, or else to
  a configured upstream Ursula node picked at random, skipping upstreams that
  recently failed at the transport.
- Buffers request bodies up to `--max-request-body-bytes`; larger requests
  receive `413 Payload Too Large` before reaching an upstream.
- Follows Ursula Raft leader redirects internally when the response includes
  `x-ursula-raft-leader-id` and the leader URL matches a configured upstream.
- Returns other redirects to clients and rewrites redirect targets so clients
  continue through the gateway.
- Keeps long-lived SSE reads open after response headers arrive.
- Optionally classifies Durable Streams requests as an explicit
  `bucket/stream/action` resource before forwarding them to provider-neutral
  authentication and authorization hooks.

## Access Control and Logical Tenancy

Ursula keeps the public resource model at two levels: `/{bucket}/{stream}`.
Shared hosted deployments can treat the bucket as the logical tenant or owner
namespace, while a standalone deployment can keep using bucket names exactly as
before.

`Gateway::new` does not enable access control. The stock `ursula gateway` role
therefore remains a transparent, tenant-unaware gateway and requires no OAuth
configuration.

A hosted gateway can instead construct `Gateway::with_access_control` with an
`auth::AccessControl` containing:

- a `PrincipalResolver` that verifies a presented OAuth bearer token and
  normalizes it into a provider-neutral principal;
- an `Authorizer` that evaluates the optional principal, explicit bucket and
  stream, and requested action.

Anonymous requests still reach the authorizer, allowing public buckets to be
read without a token. An authorizer can return `ConcealAsNotFound` for a private
bucket the caller cannot see. Validated bearer credentials terminate at the
gateway and are not forwarded to Ursula nodes. Once access control is enabled,
unrecognized and internal Ursula paths fail closed with `404`; a hosted binary
can mount its own health or operator routes outside the gateway fallback.

## Redirects

The gateway follows Ursula `307 Temporary Redirect` responses internally when
they are marked as Raft leader redirects with `x-ursula-raft-leader-id` and the
leader target resolves to one of the configured upstreams.

Other `307` responses remain visible to the client. Their `Location` header is
reduced to the path and query so redirect-following clients such as `curl -L`
continue through the gateway instead of connecting to internal Ursula nodes
directly.

## SSE Behavior

`live=sse` responses stream through the gateway. `--response-header-timeout`
only covers the time needed to receive response headers; streamed response
bodies remain open.

Example:

```bash
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Run

Start a gateway in front of one or more Ursula HTTP or HTTPS nodes:

```bash
cargo run -p ursula --bin ursula -- gateway \
  --listen 127.0.0.1:8080 \
  --upstream http://127.0.0.1:4437 \
  --upstream http://127.0.0.1:4438 \
  --upstream http://127.0.0.1:4439
```

Then send normal Durable Streams requests to the gateway:

```bash
curl -X PUT http://127.0.0.1:8080/demo
curl -X PUT http://127.0.0.1:8080/demo/hello

curl -X POST http://127.0.0.1:8080/demo/hello \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'hello world'

curl 'http://127.0.0.1:8080/demo/hello?offset=-1'
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Options

```text
--listen <ADDR>
    Address to bind. Defaults to 0.0.0.0:4437.

--upstream <URL>
    Base URL for an Ursula HTTP or HTTPS node. Repeat once per node. Required.

--response-header-timeout <SECONDS>
    Timeout for sending the upstream request and receiving response headers.
    Streamed response bodies such as SSE live reads are not covered.
    Defaults to 30.

--connect-timeout <SECONDS>
    TCP connect timeout per upstream request attempt. Defaults to 5.

--upstream-tcp-user-timeout <SECONDS>
    How long data sent to an upstream may stay unacknowledged before the
    connection closes and its request fails with 502 (Linux TCP_USER_TIMEOUT).
    Idle connections are probed three times within it: first after 2/5 of it,
    then every 1/5, each at least 1 s. 0.7.0 effectively used reqwest's 30 s
    default. Defaults to 5. At least 1.

--max-request-body-bytes <BYTES>
    Maximum request body bytes buffered for leader-redirect replay.
    Larger requests return 413 before upstream forwarding. Defaults to 33554432.

--graceful-shutdown-timeout <SECONDS>
    Maximum graceful shutdown drain time after SIGTERM/CTRL-C. Defaults to 3600.
```

## Operational Notes

- Upstream URLs support `http` and `https`.
- A node that vanishes without closing its connections, such as a
  force-deleted pod or a powered-off host, fails the requests waiting on them
  within `--upstream-tcp-user-timeout` with `502`. The request may have
  reached the node, so the client decides whether to retry it.
- An upstream that fails at the transport (refuses or times out the
  connection, drops the exchange, or sends no response headers in time) is
  skipped for 10 s, doubling up to 60 s while it keeps failing. Once a window
  expires, one request probes it, and the others keep skipping it until that
  probe answers or fails. Any response puts it back. If every upstream is
  skipped, requests go to all of them again rather than to none.
- `/__ursula/gateway/metrics` reports `upstream_ejections` (times an upstream
  was skipped after a failure) and `ejected_upstreams` (skipped now), next to
  the leader-cache counters.
- Request bodies are buffered up to `--max-request-body-bytes` because internal
  leader redirects require replaying the request to a different upstream.
- The gateway keeps no durable state and does not maintain cluster membership.
  Its leader cache and upstream ejections live in memory and start empty.
- `RUST_LOG=ursula_gateway=debug` enables request forwarding and redirect logs.

## Verify

```bash
cargo fmt --all -- --check
cargo test -p ursula-gateway
cargo clippy -p ursula-gateway --all-targets -- -D warnings
```
