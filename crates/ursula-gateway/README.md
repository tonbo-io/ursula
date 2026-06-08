# Ursula Gateway

`ursulagw` is a small HTTP/SSE gateway for Ursula clusters. It gives public
clients one stable HTTP endpoint while keeping internal Ursula node addresses
out of public responses.

The gateway is intentionally thin: it does not parse the Durable Streams
Protocol, own stream routing state, terminate SSE streams, or cache Raft
leaders. It buffers request bodies up to a configured limit only so it can
replay requests when Ursula identifies the Raft leader.

## Behavior

- Picks one configured upstream Ursula node for each incoming request.
- Buffers request bodies up to `--max-request-body-bytes`; larger requests
  receive `413 Payload Too Large` before reaching an upstream.
- Follows Ursula Raft leader redirects internally when the response includes
  `x-ursula-raft-leader-id` and the leader URL matches a configured upstream.
- Returns other redirects to clients and rewrites redirect targets so clients
  continue through the gateway.
- Keeps long-lived SSE reads open after response headers arrive.

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
cargo run -p ursula-gateway --bin ursulagw -- \
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

--max-request-body-bytes <BYTES>
    Maximum request body bytes buffered for leader-redirect replay.
    Larger requests return 413 before upstream forwarding. Defaults to 33554432.

--graceful-shutdown-timeout <SECONDS>
    Maximum graceful shutdown drain time after SIGTERM/CTRL-C. Defaults to 3600.
```

## Operational Notes

- Upstream URLs support `http` and `https`.
- Upstreams are selected randomly per request.
- Request bodies are buffered up to `--max-request-body-bytes` because internal
  leader redirects require replaying the request to a different upstream.
- The gateway is stateless. It does not cache Raft leaders or maintain cluster
  membership.
- `RUST_LOG=ursula_gateway=debug` enables request forwarding and redirect logs.

## Verify

```bash
cargo fmt --all -- --check
cargo test -p ursula-gateway
cargo clippy -p ursula-gateway --all-targets -- -D warnings
```
